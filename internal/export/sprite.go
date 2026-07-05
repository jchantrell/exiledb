package export

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
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
		Path:       "Art/UIImages1.txt",
		NamePrefix: "Art/2DArt/UIImages/",
	},
	{
		Path:       "Art/UIDivinationImages.txt",
		NamePrefix: "Art/2DItems/Divination/Images/",
	},
	{
		Path:       "Art/UIShopImages.txt",
		NamePrefix: "Art/2DArt/Shop/",
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
			Name:       matches[1],
			SpritePath: matches[2],
			Left:       left,
			Top:        top,
			Width:      right - left + 1,
			Height:     bottom - top + 1,
		})
	}

	return sprites, nil
}

func DecodeUTF16LE(data []byte) (string, error) {
	if len(data)%2 != 0 {
		return "", fmt.Errorf("invalid UTF-16LE data: odd number of bytes")
	}

	u16 := make([]uint16, len(data)/2)
	for i := 0; i < len(u16); i++ {
		u16[i] = uint16(data[i*2]) | uint16(data[i*2+1])<<8
	}

	return string(utf16.Decode(u16)), nil
}

func IsInsideSprite(path string) bool {
	for _, list := range SpriteLists {
		if strings.HasPrefix(path, list.NamePrefix) {
			return true
		}
	}
	return false
}
