package export

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type SpriteImage struct {
	Name       string
	SpritePath string
	Top        int
	Left       int
	Width      int
	Height     int
}

type SpriteList struct {
	Path       string
	NamePrefix string
}

var SpriteLists = []SpriteList{
	{
		Path:       "art/uiimages1.txt",
		NamePrefix: "art/2dart/uiimages/",
	},
	{
		Path:       "art/uidivinationimages.txt",
		NamePrefix: "art/2ditems/divination/images/",
	},
	{
		Path:       "art/uishopimages.txt",
		NamePrefix: "art/2dart/shop/",
	},
}

// spriteLinePattern matches the sprite index line format:
// "name" "spritePath" x1 y1 x2 y2  (left top right bottom, inclusive)
var spriteLinePattern = regexp.MustCompile(`^"([^"]+)" "([^"]+)" ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+)$`)

func ParseSpriteIndex(data []byte) ([]SpriteImage, error) {
	text, err := DecodeUTF16LE(data)
	if err != nil {
		return nil, fmt.Errorf("decoding UTF-16LE: %w", err)
	}

	return parseSpriteText(text)
}

func parseSpriteText(text string) ([]SpriteImage, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return []SpriteImage{}, nil
	}

	lines := strings.Split(text, "\n")
	sprites := make([]SpriteImage, 0, len(lines))

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		matches := spriteLinePattern.FindStringSubmatch(line)
		if matches == nil {
			return nil, fmt.Errorf("line %d: invalid sprite format", i+1)
		}

		left, err := strconv.Atoi(matches[3])
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid left value: %w", i+1, err)
		}

		top, err := strconv.Atoi(matches[4])
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid top value: %w", i+1, err)
		}

		right, err := strconv.Atoi(matches[5])
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid right value: %w", i+1, err)
		}

		bottom, err := strconv.Atoi(matches[6])
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid bottom value: %w", i+1, err)
		}

		sprites = append(sprites, SpriteImage{
			Name:       strings.ToLower(matches[1]),
			SpritePath: strings.ToLower(matches[2]),
			Left:       left,
			Top:        top,
			Width:      right - left + 1,
			Height:     bottom - top + 1,
		})
	}

	return sprites, nil
}

func IsInsideSprite(path string) bool {
	for _, list := range SpriteLists {
		if strings.HasPrefix(path, list.NamePrefix) {
			return true
		}
	}
	return false
}
