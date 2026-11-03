// Package writerlog records the closed lifecycle of writes performed while a
// caller retains a panel lock. It sends only fixed metadata and the retained
// lock descriptor to a configured local mediator; it never sends typed input,
// screen contents, command arguments, or content lengths.
package writerlog
