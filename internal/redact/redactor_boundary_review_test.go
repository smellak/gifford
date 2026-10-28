// Independent regressions for rule-less redactors and text control validation.
package redact

import (
	"errors"
	"reflect"
	"testing"
)

type reviewNoFindingsRule struct{}

func (reviewNoFindingsRule) find([]string) []finding { return nil }

func TestReviewRedactorBoundaries(t *testing.T) {
	t.Parallel()
	for i, r := range []*Redactor{nil, {}} {
		text, err := r.RedactText("")
		if !errors.Is(err, ErrInternal) || !reflect.DeepEqual(text, TextResult{}) {
			t.Errorf("uninitialized redactor %d accepted empty text: result=%#v err=%v", i, text, err)
		}
		screen, err := r.RedactScreen(Screen{})
		if !errors.Is(err, ErrInternal) || !reflect.DeepEqual(screen, ScreenResult{}) {
			t.Errorf("uninitialized redactor %d accepted empty screen: result=%#v err=%v", i, screen, err)
		}
	}
	ready := &Redactor{rules: &ruleSet{version: "review", rules: []rule{reviewNoFindingsRule{}}}}
	for _, control := range []rune{0x80, 0x85, 0x9b, 0x9f} {
		for i, r := range []*Redactor{nil, ready} {
			result, err := r.RedactText("public" + string(control) + "text")
			if !errors.Is(err, ErrInvalidInput) || !reflect.DeepEqual(result, TextResult{}) {
				t.Errorf("redactor %d: U+%04X must fail input validation before rules; got %v", i, control, err)
			}
		}
	}
	for _, input := range []string{"", "plain text", "a\tb\r\nc", "中文"} {
		result, err := ready.RedactText(input)
		if err != nil || result.Text != input || result.PolicyVersion != "review" {
			t.Errorf("valid synthetic input must pass unchanged under test-only rules: %v", err)
		}
	}
}
