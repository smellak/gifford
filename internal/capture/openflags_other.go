//go:build !unix

package capture

// Without the flag the check after open still refuses a special file.
const noBlock = 0
