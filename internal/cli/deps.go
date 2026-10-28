package cli

// Deps holds the dependencies that subcommands share. It grows as packages
// land (config service, clock, logger); subcommands never read globals.
type Deps struct{}
