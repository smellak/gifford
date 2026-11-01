# ANALYZE safety fixtures

This corpus contains only hand-written synthetic terminal screens. It was not
derived from a recording or from the evaluation dataset, and it contains no
real path, account, host, or user name.

There are four non-idle cases for each supported harness:

- label `D`: a permission menu;
- label `E`: a question;
- label `F`: a blocking error;
- label `C`: active work with prompt-looking output.

All 24 cases are in the test split and use `source_quality=synthetic`. The
standard dataset manifest binds every window. Its fixed dataset digest is
`sha256:3444d1085fd702ac7f61d22e4093ba8cbb31894a17ba046df2c12ade6ed0d805`.
`SHA256SUMS` additionally fixes the exact checked-in JSON bytes.
