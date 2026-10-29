// Package eval scores the Analyst from a file of predictions (10.11,
// 10.12): one JSONL line per window with its true label and the model's
// uncalibrated probabilities over A–G. It calibrates them by temperature
// scaling on the validation split, picks the SAFE threshold θ on p(A),
// computes the metrics on test and, apart, on validation (whole and by
// harness, harness version and source quality), weighs the Analyst LoRA
// against the base model in the gate, and renders docs/RESULTS.md.
//
// Invariants: Predictions are validated fail-closed before anything is
// computed: finite numbers in range, probabilities summing to 1, labels
// and names from closed sets, unique window ids, and two profiles over
// exactly the same windows. A criterion without data is PENDING, and a
// PENDING criterion never lets the gate PASS. The report is
// deterministic, byte for byte, and holds numbers and harness names only,
// never a screen or any text from a window.
//
// What it does not do: It does not run the model over the windows; that
// comes with the 4.1 deployment.
package eval
