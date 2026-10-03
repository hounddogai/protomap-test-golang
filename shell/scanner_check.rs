#[test]
fn test_exports_fixture_scan() {
    use protomap_model::artifact::{Endpoint, IslandKind};
    use std::fmt::Write;
    use std::path::Path;
    use std::time::Instant;

    let root = Path::new(FIXTURE_ROOT_LITERAL);
    let started = Instant::now();
    let last_stage = std::cell::RefCell::new(String::new());
    let artifact = protomap_scanner::scan_artifact(root, &|stage| {
        if last_stage.borrow().as_str() != stage {
            eprintln!("{stage}");
            last_stage.replace(stage.to_owned());
        }
        Ok(())
    })
    .unwrap();
    let elapsed = started.elapsed().as_secs_f64();
    let encoded = artifact.encode().unwrap();
    let repeated = protomap_scanner::with_threads(1, || protomap_scanner::scan_artifact(root, &|_| Ok(()))).unwrap();
    assert!(encoded == repeated.encode().unwrap(), "fresh scans differ across thread counts");
    let body = &artifact.body;
    let mut report = String::new();
    writeln!(report, "STATS\t{elapsed}\t{}", encoded.len()).unwrap();
    for island in &body.islands {
        if island.kind != IslandKind::Service {
            continue;
        }
        // A service may implement several methods, each a handler reaching call sites of its own.
        for (handler_index, handler) in island.handlers.iter().enumerate() {
            let handler_index = handler_index as u32;
            let method = body.text(handler.method);
            writeln!(report, "SERVICE\t{method}").unwrap();
            for &index in &handler.call_sites {
                let call = &island.call_sites[index as usize];
                let target = call.method.map(|name| body.text(name)).unwrap_or("UNRESOLVED");
                let request_flow = island.flows.iter().any(|flow| {
                    matches!(flow.source, Endpoint::HandlerRequest { handler, .. } if handler == handler_index)
                        && matches!(flow.target, Endpoint::CallSiteRequest { call_site, .. } if call_site == index)
                        && flow.source.path().last().is_some_and(|name| body.text(*name).ends_with(".email"))
                        && flow.target.path().last().is_some_and(|name| body.text(*name).ends_with(".email"))
                        && flow.certain
                });
                writeln!(report, "EDGE\t{method}\t{target}\t{request_flow}").unwrap();
            }
            let response_flow = island.flows.iter().any(|flow| {
                let reached = |call_site: u32| handler.call_sites.contains(&call_site);
                matches!(flow.source, Endpoint::CallSiteResponse { call_site, .. } if reached(call_site))
                    && matches!(flow.target, Endpoint::HandlerResponse { handler, .. } if handler == handler_index)
                    && flow.certain
            });
            writeln!(report, "RESPONSE\t{method}\t{response_flow}").unwrap();
            if handler.call_sites.is_empty() {
                let leaf_response_flow = island.flows.iter().any(|flow| {
                    matches!(flow.source, Endpoint::HandlerRequest { handler, .. } if handler == handler_index)
                        && matches!(flow.target, Endpoint::HandlerResponse { handler, .. } if handler == handler_index)
                        && flow.source.path().last().is_some_and(|name| body.text(*name).ends_with(".email"))
                        && flow.target.path().last().is_some_and(|name| body.text(*name).ends_with(".email"))
                        && flow.certain
                });
                writeln!(report, "LEAF_RESPONSE\t{method}\t{leaf_response_flow}").unwrap();
            }
        }
    }
    std::fs::write(REPORT_PATH_LITERAL, report).unwrap();
}
