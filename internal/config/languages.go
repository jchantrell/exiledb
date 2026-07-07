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

var supportedLanguages = []string{
	LanguageEnglish,
	LanguageFrench,
	LanguageGerman,
	LanguageSpanish,
	LanguagePortuguese,
	LanguageRussian,
	LanguageThai,
	LanguageJapanese,
	LanguageKorean,
	LanguageTraditionalChinese,
	LanguageSimplifiedChinese,
}

func SupportedLanguages() []string {
	return append([]string(nil), supportedLanguages...)
}

func isValidLanguage(language string) bool {
	for _, l := range supportedLanguages {
		if l == language {
			return true
		}
	}
	return false
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
