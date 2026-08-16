# Structural Rust checks

These rules cover a few structural patterns that rustfmt and Clippy do not
express well. Run them with:

```sh
mise run check
```

Error-severity rules fail the task. Warnings need review and may be
intentional. The structural gate runs both the fixture tests and a scan of
repository Rust sources. The scan also warns when a Rust file reaches 500
lines; that warning is advisory so a cohesive module can remain intact while
still being visible during maintenance review.

| Signal | Tool | Severity | Threshold |
| --- | --- | --- | --- |
| Deep block nesting | Clippy `excessive_nesting` | error | `4` |
| Long `if` / `else if` cascade | `rust-elseif-cascade` | error | 3 branches |
| Ordered `if let Some(...)` cascade | `rust-if-let-policy-cascade` | warning | 2 guards |
| Dense `if let Some(...)` cascade | `rust-if-let-policy-cascade-dense` | error | 3 guards |
| Missing blank after control flow | `rust-block-spacing` | error | — |

The checks are heuristics for control-flow readability and implicit policy.
They identify structures that deserve a local decision; they do not prove that
the matching code is incorrect.

Suppress a true positive next to the code and name the rule:

```rust
// ast-grep-ignore: rust-if-let-policy-cascade
if let Some(value) = first {
    use_value(value);
}
```
