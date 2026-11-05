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
	font := map[rune][7]string{
		'A': {"  /\\  ", " /  \\ ", "/    \\", "|    |", "|----|", "|    |", "|    |"},
		'E': {"|-----", "|     ", "|     ", "|---- ", "|     ", "|     ", "|-----"},
		'G': {" /----", "|     ", "|     ", "|  ---|", "|    |", "|    |", " \\----"},
		'I': {"------", "  ||  ", "  ||  ", "  ||  ", "  ||  ", "  ||  ", "------"},
		'M': {"|\\   /|", "| \\ / |", "|  \\/ |", "|     |", "|     |", "|     |", "|     |"},
		'N': {"|\\    |", "| \\   |", "|  \\  |", "|   \\ |", "|    \\|", "|     |", "|     |"},
		'O': {" /---\\ ", "|     |", "|     |", "|     |", "|     |", "|     |", " \\---/ "},
		'P': {"|-----\\", "|     |", "|-----/", "|      ", "|      ", "|      ", "|      "},
		'R': {"|-----\\", "|     |", "|-----/", "|  \\   ", "|   \\  ", "|    \\ ", "|     \\"},
		'S': {" /-----", "|      ", "|      ", " \\---- ", "      |", "      |", "------/"},
		'T': {"------|", "   |   ", "   |   ", "   |   ", "   |   ", "   |   ", "   |   "},
		'U': {"|     |", "|     |", "|     |", "|     |", "|     |", " \\   / ", "  \\_/  "},
		'V': {"|     |", " \\   / ", "  \\ /  ", "   \\_/ ", "       ", "       ", "       "},
		'W': {"|     |", "|     |", "|  |  |", "| \\_/ |", "| / \\ |", "|/   \\|", "|     |"},
	}
	var banner strings.Builder
	for _, word := range strings.Fields(strings.ToUpper(service)) {
		for row := range 7 {
			for _, char := range word {
				if glyph, ok := font[char]; ok {
					banner.WriteString(glyph[row])
					banner.WriteByte(' ')
				}
			}
			banner.WriteByte('\n')
		}
		banner.WriteByte('\n')
	}
	fmt.Printf("\033[1;36m%s\033[0mVersion: %s | Port: :%s\n\n", banner.String(), version, port)
}
