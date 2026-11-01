// Package eval scores the Analyst from a file of predictions (10.11,
// 10.12): one JSONL line per window with its true label and the model's
// uncalibrated probabilities over A–G. Legacy reports calibrate on validation.
// Partitioned checkpoint reports fit temperature and the SAFE threshold θ on
// fixed val-cal sessions and compare only on val-cmp. Partitioned final reports
// consume those sealed values without refitting, decide criteria 1–4 only on
// fixed blind test sessions, and report the remaining historical test sessions
// separately. These partitioned modes apply only to the Analyst LoRA adapter;
// prompt comparison retains its legacy validation/test path and rejects their
// inputs. A separately sealed synthetic development corpus can be scored only
// with frozen calibration through ScoreDevelopment; it never enters fitting,
// val-cmp or a gate. Reports are whole and by harness, harness version and
// source quality. Prompt comparisons also replay the output-quiet guard and run
// a fixed synthetic safety suite without fitting on it.
//
// Invariants: Predictions are validated fail-closed before anything is
// computed: finite numbers in range, probabilities summing to 1, labels
// and names from closed sets, unique window ids, and two profiles over
// exactly the same windows. Prompt A/B files additionally carry one complete,
// serial run identity and must use the same served model and runtime. A
// criterion without data is PENDING, and a PENDING criterion never lets the
// gate PASS. An inapplicable criterion is N/A and is ignored. The report is
// deterministic, byte for byte, and holds numbers and harness names only,
// never a screen or any text from a window.
//
// Predict (10.11) writes those files: it runs the model, through the
// gateway as the Analyst does, over the validation and test windows of a
// manifest, each checked against its entry first. A failed call is an
// invalid line with a fixed ErrorClass, never dropped; an answer from
// another inference profile aborts the run. The file holds ids, numbers,
// run provenance and error classes only.
package eval
