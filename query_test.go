package routeros_test

import (
	"reflect"
	"testing"

	"github.com/filydottech/ros-go"
)

func TestQueryExpressionsBuildLeafWords(t *testing.T) {
	tests := []struct {
		name string
		expr routeros.Expr
		want []string
	}{
		{
			name: "equal",
			expr: routeros.Equal("name", "ether1"),
			want: []string{"?name=ether1"},
		},
		{
			name: "less than",
			expr: routeros.LessThan("mtu", "1500"),
			want: []string{"?<mtu=1500"},
		},
		{
			name: "greater than",
			expr: routeros.GreaterThan("mtu", "1500"),
			want: []string{"?>mtu=1500"},
		},
		{
			name: "exists",
			expr: routeros.Exists("comment"),
			want: []string{"?comment"},
		},
		{
			name: "absent",
			expr: routeros.Absent("comment"),
			want: []string{"?-comment"},
		},
		{
			name: "unrestricted field and value",
			expr: routeros.Equal("name?=value", ""),
			want: []string{"?name?=value="},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.expr.Words(); !reflect.DeepEqual(got, test.want) {
				t.Errorf("Words() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNotAppendsPostfixOperator(t *testing.T) {
	expr := routeros.Not(routeros.Equal("type", "ether"))
	want := []string{"?type=ether", "?#!"}

	if got := expr.Words(); !reflect.DeepEqual(got, want) {
		t.Errorf("Words() = %q, want %q", got, want)
	}
}

func TestQueryExpressionsComposeInPostfixOrder(t *testing.T) {
	expr := routeros.Or(
		routeros.Equal("type", "ether"),
		routeros.And(
			routeros.Absent("comment"),
			routeros.Not(routeros.Exists("disabled")),
		),
		routeros.GreaterThan("mtu", "1500"),
	)
	want := []string{
		"?type=ether",
		"?-comment",
		"?disabled",
		"?#!",
		"?#&",
		"?>mtu=1500",
		"?#|",
		"?#|",
	}

	if got := expr.Words(); !reflect.DeepEqual(got, want) {
		t.Errorf("Words() = %q, want %q", got, want)
	}
}

func TestQueryExpressionGroupWithOneOperandHasNoOperator(t *testing.T) {
	expression := routeros.Equal("name", "ether1")
	want := []string{"?name=ether1"}

	for _, test := range []struct {
		name string
		expr routeros.Expr
	}{
		{name: "and", expr: routeros.And(expression)},
		{name: "or", expr: routeros.Or(expression)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.expr.Words(); !reflect.DeepEqual(got, want) {
				t.Errorf("Words() = %q, want %q", got, want)
			}
		})
	}
}

func TestQueryExpressionWordsAreIndependent(t *testing.T) {
	first := routeros.Equal("name", "ether1")
	combined := routeros.And(first, routeros.Absent("comment"))

	firstWords := first.Words()
	firstWords[0] = "?name=changed"
	combinedWords := combined.Words()
	combinedWords[0] = "?name=changed"

	if got, want := first.Words(), []string{"?name=ether1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("first.Words() = %q, want %q", got, want)
	}
	if got, want := combined.Words(), []string{"?name=ether1", "?-comment", "?#&"}; !reflect.DeepEqual(got, want) {
		t.Errorf("combined.Words() = %q, want %q", got, want)
	}
}

func TestQueryExpressionsRejectNilOperands(t *testing.T) {
	var expression routeros.Expr

	for _, test := range []struct {
		name string
		call func()
	}{
		{name: "not", call: func() { routeros.Not(expression) }},
		{name: "and first", call: func() { routeros.And(expression) }},
		{name: "and rest", call: func() { routeros.And(routeros.Exists("name"), expression) }},
		{name: "or first", call: func() { routeros.Or(expression) }},
		{name: "or rest", call: func() { routeros.Or(routeros.Exists("name"), expression) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if got := recover(); got != "routeros: query expression is nil" {
					t.Errorf("panic = %v, want nil expression error", got)
				}
			}()
			test.call()
		})
	}
}
