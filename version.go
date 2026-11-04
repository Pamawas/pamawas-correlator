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

func printStartupBanner(service, version, port string) {
	font := map[rune][5]string{
		'a': {".#.", "#.#", "###", "#.#", "#.#"}, 'c': {"###", "#..", "#..", "#..", "###"},
		'd': {"##.", "#.#", "#.#", "#.#", "##."}, 'e': {"###", "#..", "##.", "#..", "###"},
		'g': {"###", "#..", "#.#", "###", "..#"}, 'h': {"#.#", "#.#", "###", "#.#", "#.#"},
		'i': {"###", ".#.", ".#.", ".#.", "###"}, 'l': {"#..", "#..", "#..", "#..", "###"},
		'm': {"#.#", "###", "###", "#.#", "#.#"}, 'n': {"##.", "#.#", "#.#", "#.#", "#.#"},
		'o': {"###", "#.#", "#.#", "#.#", "###"}, 'p': {"##.", "#.#", "##.", "#..", "#.."},
		'r': {"##.", "#.#", "##.", "#.#", "#.#"}, 's': {"###", "#..", "###", "..#", "###"},
		't': {"###", ".#.", ".#.", ".#.", ".#."}, 'u': {"#.#", "#.#", "#.#", "#.#", "###"},
		'w': {"#.#", "#.#", "###", "###", "#.#"}, '-': {"...", "...", "###", "...", "..."},
		' ': {"   ", "   ", "   ", "   ", "   "},
	}
	var rows [5]string
	for _, char := range strings.ToLower(service) {
		glyph, ok := font[char]
		if !ok {
			continue
		}
		for i := range rows {
			rows[i] += glyph[i] + " "
		}
	}
	fmt.Printf("\033[1;36m%s\n%s\n%s\n%s\n%s\033[0m\nVersion: %s | Port: :%s\n\n",
		rows[0], rows[1], rows[2], rows[3], rows[4], version, port)
}
