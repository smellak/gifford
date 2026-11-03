//go:build !linux

package adapter

func systemNativeSocket() string { return "" }
