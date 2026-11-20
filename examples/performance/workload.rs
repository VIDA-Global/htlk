//! Deterministic graph generators and separately timed compile/admit operations.
use crate::{Result, e2e, micros};
use htlk::{
    compiler::{CompilationSuccess, Compiler, MCPCatalogInputs, SourceBundle},
    runtime::{NativeRegistry, records::InputValue, verify_executable},
};
use htlk_executable::{
    Identifier,
    cbor::{Limits, Map, Value},
};
use std::{collections::BTreeMap, fmt::Write, hint::black_box, time::Instant};

pub struct Workload {
    shape: String,
    nodes: usize,
    payload: usize,
    delay_ms: u64,
}

impl Workload {
    pub fn new(shape: &str, nodes: usize, payload: usize, delay_ms: u64) -> Result<Self> {
        if !matches!(
            shape,
            "chain" | "wide" | "loop" | "wait" | "stdio" | "http" | "sse"
        ) || !(1..=64).contains(&nodes)
            || (shape == "loop" && nodes > 30)
            || !(8..=32_768).contains(&payload)
            || delay_ms > 2_000
            || (!matches!(shape, "chain" | "wide" | "loop") && nodes != 1)
            || (!matches!(shape, "stdio" | "http" | "sse") && delay_ms != 0)
        {
            return Err("invalid graph shape, node count (1–64, up to 30 for loop), payload size (8–32768), or peer delay (0–2000 ms)".into());
        }
        Ok(Self {
            shape: shape.into(),
            nodes,
            payload,
            delay_ms,
        })
    }

    pub fn is_wait(&self) -> bool {
        self.shape == "wait"
    }

    pub fn is_short_chain(&self) -> bool {
        self.shape == "chain" && self.nodes == 1
    }

    pub fn peer(&self) -> Option<e2e::Peer> {
        let transport = match self.shape.as_str() {
            "stdio" => "stdio",
            "http" => "http",
            "sse" => "sse",
            _ => return None,
        };
        Some(e2e::Peer::new(
            transport,
            &format!("perf-{}", self.delay_ms),
        ))
    }

    fn payload(&self, index: usize) -> String {
        format!("{index:08x}{}", "x".repeat(self.payload - 8))
    }

    pub fn input(&self, index: usize) -> BTreeMap<Identifier, InputValue> {
        let value = match self.shape.as_str() {
            "loop" => Value::Integer(0),
            "wait" => self.object("request", index),
            _ => Value::Text(self.payload(index)),
        };
        BTreeMap::from([("seed".parse().unwrap(), InputValue::Inline(value))])
    }

    pub fn expected(&self, index: usize) -> Value {
        match self.shape.as_str() {
            "loop" => Value::Integer(self.nodes as i64),
            "wait" => Value::Text("host reply".into()),
            "stdio" | "http" | "sse" => self.object("value", index),
            _ => Value::Text(self.payload(index)),
        }
    }

    fn object(&self, key: &str, index: usize) -> Value {
        Value::Map(Map::try_from_entries([(key.into(), Value::Text(self.payload(index)))]).unwrap())
    }

    pub fn source(&self) -> String {
        match self.shape.as_str() {
            "loop" => self.loop_source(),
            "wait" => r#"ir_version="0.1" graph perf.wait {
                inputs={seed=json} outputs={value=string}
                nodes{ review=wait(string){topic="perf" timeout_ms=50000} }
                edges{edge request{from=inputs.seed to=review.inputs.request} edge reply{from=review.outputs.value to=outputs.value}}
            }"#.into(),
            "stdio" | "http" | "sse" => r#"ir_version="0.1" graph perf.mcp {
                inputs={seed=string} outputs={value=json}
                nodes{ args=eval(json,{value=inputs.seed}){inputs={seed=string}} echo=call(mcp.tool("fixture","echo"))
                       project=eval(json,inputs.result.data){inputs={result=McpToolResult}} }
                edges{edge input{from=inputs.seed to=args.inputs.seed} edge args{from=args.outputs.value to=echo.inputs.arguments}
                      edge result{from=echo.outputs.value to=project.inputs.result} edge value{from=project.outputs.value to=outputs.value}}
            }"#.into(),
            _ => self.local_source(),
        }
    }

    fn loop_source(&self) -> String {
        let mut nodes = String::new();
        let mut edges = String::new();
        for i in 0..self.nodes {
            writeln!(nodes, "n{i}=eval(integer,{})", i + 1).unwrap();
            for (name, to) in [("next", "next.counter"), ("value", "outputs.value")] {
                writeln!(
                    edges,
                    "edge {name}{i}{{from=n{i}.outputs.value to={to} when=carried.counter=={i}}}"
                )
                .unwrap();
            }
        }
        format!(
            r#"ir_version="0.1" graph perf.loop {{
            inputs={{seed=integer}} outputs={{value=integer}}
            nodes{{ repeat=loop{{inputs={{seed=integer}} outputs={{value=integer}} carried={{counter=inputs.seed}}
                body{{nodes{{{nodes}}} edges{{{edges}}}}}
                until=next.counter=={} max_iterations={}
            }}}} edges{{edge seed{{from=inputs.seed to=repeat.inputs.seed}} edge value{{from=repeat.outputs.value to=outputs.value}}}}
        }}"#,
            self.nodes, self.nodes
        )
    }

    fn local_source(&self) -> String {
        let wide = self.shape == "wide";
        let mut source = format!(
            r#"ir_version="0.1" graph perf.{} {{inputs={{seed=string}} outputs={{value=string}} nodes{{"#,
            self.shape
        );
        let mut edges = String::new();
        for i in 0..self.nodes {
            writeln!(
                source,
                "n{i}=eval(string,inputs.seed){{inputs={{seed=string}}}}"
            )
            .unwrap();
            let from = if i == 0 || wide {
                "inputs.seed".to_owned()
            } else {
                format!("n{}.outputs.value", i - 1)
            };
            writeln!(edges, "edge in{i}{{from={from} to=n{i}.inputs.seed}}").unwrap();
        }
        let out = if wide {
            source.push_str("join=eval(string,inputs.p0){inputs={");
            for i in 0..self.nodes {
                if i != 0 {
                    source.push(',');
                }
                write!(source, "p{i}=string").unwrap();
                writeln!(
                    edges,
                    "edge out{i}{{from=n{i}.outputs.value to=join.inputs.p{i}}}"
                )
                .unwrap();
            }
            source.push_str("}}\n");
            "join.outputs.value".to_string()
        } else {
            format!("n{}.outputs.value", self.nodes - 1)
        };
        write!(
            source,
            "}} edges{{{edges} edge result{{from={out} to=outputs.value}}}}}}"
        )
        .unwrap();
        source
    }
}

pub fn compiler_samples(
    compiler: &Compiler,
    registry: &NativeRegistry,
    source: &str,
    catalogs: &MCPCatalogInputs,
    graph: &CompilationSuccess,
    samples: usize,
) -> Result<()> {
    if samples == 0 {
        return Ok(());
    }
    let limits = Limits::default();
    let decision_catalogs = crate::support::catalogs();
    let bundle = include_bytes!("../../specs/examples/modules/source_bundle.json");
    let standalone = include_str!("../../specs/examples/decision_brief.htlk");
    let reference = compiler
        .compile_source(standalone, &decision_catalogs)?
        .fingerprint;
    for _ in 0..samples {
        let begin = Instant::now();
        let compiled = compiler.compile_source(black_box(source), catalogs)?;
        let elapsed = begin.elapsed();
        if compiled.fingerprint != graph.fingerprint {
            return Err("compiler fingerprint changed".into());
        }
        println!("sample,compile_graph,{},{}", micros(elapsed), source.len());

        let begin = Instant::now();
        let verified = verify_executable(black_box(&graph.executable_bytes), registry, &limits)?;
        let elapsed = begin.elapsed();
        if verified.fingerprint() != graph.fingerprint {
            return Err("runtime verification diverged".into());
        }
        println!(
            "sample,verify_executable,{},{}",
            micros(elapsed),
            graph.executable_bytes.len()
        );

        let begin = Instant::now();
        let encoded = compiled.canonical_document.encode(&limits)?;
        let elapsed = begin.elapsed();
        println!(
            "sample,encode_document,{},{}",
            micros(elapsed),
            black_box(encoded.len())
        );

        let begin = Instant::now();
        let plain = compiler.compile_source(standalone, &decision_catalogs)?;
        let elapsed = begin.elapsed();
        if plain.fingerprint != reference {
            return Err("standalone fingerprint changed".into());
        }
        println!(
            "sample,compile_standalone,{},{}",
            micros(elapsed),
            standalone.len()
        );

        let begin = Instant::now();
        let modular = compiler.compile_bundle(
            &SourceBundle::from_json(bundle, &limits)?,
            &decision_catalogs,
        )?;
        let elapsed = begin.elapsed();
        if modular.fingerprint != reference {
            return Err("bundle did not match equivalent standalone source".into());
        }
        println!("sample,compile_bundle,{},{}", micros(elapsed), bundle.len());
    }
    Ok(())
}
