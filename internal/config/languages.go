package config

import "fmt"

const (
	LanguageEnglish            = "English"
	LanguageFrench             = "French"
	LanguageGerman             = "German"
	LanguageSpanish            = "Spanish"
	LanguagePortuguese         = "Portuguese"
	LanguageRussian            = "Russian"
	LanguageThai               = "Thai"
	LanguageJapanese           = "Japanese"
	LanguageKorean             = "Korean"
	LanguageTraditionalChinese = "Traditional Chinese"
	LanguageSimplifiedChinese  = "Simplified Chinese"
)

var validLanguages = map[string]bool{
	LanguageEnglish:            true,
	LanguageFrench:             true,
	LanguageGerman:             true,
	LanguageSpanish:            true,
	LanguagePortuguese:         true,
	LanguageRussian:            true,
	LanguageThai:               true,
	LanguageJapanese:           true,
	LanguageKorean:             true,
	LanguageTraditionalChinese: true,
	LanguageSimplifiedChinese:  true,
}

func isValidLanguage(language string) bool {
	return validLanguages[language]
}

func validateLanguages(languages []string) error {
	if len(languages) == 0 {
		return nil // Empty list is valid, will default to English
	}

	for _, lang := range languages {
		if lang == "" {
			return fmt.Errorf("language name cannot be empty")
		}

		if !isValidLanguage(lang) {
			return fmt.Errorf("unsupported language '%s': supported languages are English, French, German, Spanish, Portuguese, Russian, Thai, Japanese, Korean, Traditional Chinese, Simplified Chinese", lang)
		}
	}

	return nil
}
