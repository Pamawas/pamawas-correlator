package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
)

func loadVersion() string {
	if content, err := os.ReadFile("version.txt"); err == nil {
		if version := strings.TrimSpace(string(content)); version != "" {
			return version
		}
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return "dev"
}

func printStartupBanner(version, port string) {
	banner := ` ____      _    __  __      _    __        __     _     ____
|  _ \    / \  |  \/  |   / \  \ \      / /    / \   / ___|
| |_) |  / _ \ | |\/| |  / _ \  \ \ /\ / /    / _ \  \___ \
|  __/  / ___ \| |  | | / ___ \  \ V  V /    / ___ \  ___) |
|_|    /_/   \_\_|  |_|/_/   \_\  \_/\_/    /_/   \_\|____/`
	serviceArt := `  _____    ___    _____    _____    _____   _          _     _____    ___    _____
 / ____|  / _ \  |  __ \  |  __ \  | ____| | |        / \   |_   _|  / _ \  |  __ \
| |      | | | | | |__) | | |__) | |  _|   | |       / _ \    | |   | | | | | |__) |
| |      | |_| | |  _  /  |  _  /  | |___  | |____  / ___ \   | |   | |_| | |  _  /
 \_____|  \___/  |_| \_\ |_| \_\  |_____| |______| /_/   \_\ |_|    \___/  |_| \_\`
	fmt.Printf("\033[1;36m%s\n%s\033[0m\nVersion: %s | Port: :%s\n\n", banner, serviceArt, version, port)
}
