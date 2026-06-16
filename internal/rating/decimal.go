package rating

import (
	"fmt"
	"math/big"
	"regexp"
)

// moneyScale is the number of fractional decimal digits money is stored and
// compared at, matching the NUMERIC(20,9) columns in migrations/0002_rating.sql.
// 9 digits == nano-USD resolution. Dec.Round rounds to this scale (half-up) so
// the loaded prices, the oracle, and the DB's NUMERIC arithmetic all agree.
const moneyScale = 9

// Dec is an EXACT decimal money value backed by a big.Rat. It is the type the
// YAML price loader parses per-token rates into (never float — a float round-trip
// silently corrupts money) and the type the Rate() oracle does its exact arithmetic
// in. Dec uses big.Rat (exact rationals), so multiplies and adds have ZERO rounding
// error; rounding happens once, explicitly, at Round(moneyScale).
//
// IMPORTANT: Dec is for PARSE/VALIDATE/ORACLE use. The production rating money MATH
// still happens in SQL (the rater multiplies and sums NUMERIC in one statement) —
// Dec carries a per-token rate from the YAML file to the DB as a canonical decimal
// STRING (see String), never as a Go float. See doc.go for the production-vs-oracle
// split.
//
// The zero Dec is exact 0.
type Dec struct {
	// r is nil for the zero value (treated as 0). Always accessed via rat().
	r *big.Rat
}

// rat returns the underlying rational, treating the nil zero-value as 0.
func (d Dec) rat() *big.Rat {
	if d.r == nil {
		return new(big.Rat) // 0/1
	}
	return d.r
}

// decimalRe pins ParseDec to a plain fixed-point decimal: an optional sign, digits,
// an optional fractional part. It REJECTS float-style exponents ("1e-6"), hex, and
// rationals ("3/2") — all forms big.Rat.SetString would otherwise accept — so the
// money path can never ingest a value that looks like a float. A price authored in
// the operator's YAML file must be an exact decimal literal or the load fails closed.
var decimalRe = regexp.MustCompile(`^[+-]?[0-9]+(\.[0-9]+)?$`)

// MustDec parses a decimal string (e.g. "0.000000150") into an exact Dec, panicking
// on a malformed input. For test fixtures and constants where the literal is known
// good. Production code (the price loader) uses ParseDec and surfaces the error.
func MustDec(s string) Dec {
	d, err := ParseDec(s)
	if err != nil {
		panic(err)
	}
	return d
}

// ParseDec parses a fixed-point decimal string into an exact Dec. It accepts the
// forms Postgres emits for a NUMERIC ("3", "0.300000000", "-0.5") and the forms an
// operator writes in the price YAML ("0.000003", "1.5"); it REJECTS float-style
// exponents, fractions, and anything else (see decimalRe) to keep the money path
// free of any float round-trip. Fails closed: a malformed price string is an error,
// never a silent zero.
func ParseDec(s string) (Dec, error) {
	if s == "" {
		return Dec{}, fmt.Errorf("rating: empty decimal string")
	}
	if !decimalRe.MatchString(s) {
		return Dec{}, fmt.Errorf("rating: invalid decimal %q (must be a plain fixed-point decimal, no exponent)", s)
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return Dec{}, fmt.Errorf("rating: invalid decimal %q", s)
	}
	return Dec{r: r}, nil
}

// Add returns d + o (exact).
func (d Dec) Add(o Dec) Dec {
	return Dec{r: new(big.Rat).Add(d.rat(), o.rat())}
}

// Mul returns d * o (exact).
func (d Dec) Mul(o Dec) Dec {
	return Dec{r: new(big.Rat).Mul(d.rat(), o.rat())}
}

// MulInt returns d * n (exact), the per-token-price × token-count operation.
func (d Dec) MulInt(n int64) Dec {
	return Dec{r: new(big.Rat).Mul(d.rat(), new(big.Rat).SetInt64(n))}
}

// Round returns d rounded to `scale` fractional decimal digits, half-up (round
// half away from zero), matching the rounding the DB applies when storing into a
// NUMERIC(_, scale). This is the ONLY place rounding happens.
func (d Dec) Round(scale int) Dec {
	// scaled = d * 10^scale, then round to nearest integer half-up, then / 10^scale.
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	powRat := new(big.Rat).SetInt(pow)
	scaled := new(big.Rat).Mul(d.rat(), powRat)

	num := scaled.Num()
	den := scaled.Denom()
	// Integer division with half-up rounding on |num/den|.
	q := new(big.Int)
	rem := new(big.Int)
	q.QuoRem(num, den, rem)
	// 2*|rem| >= |den| → round away from zero.
	twiceRem := new(big.Int).Abs(rem)
	twiceRem.Lsh(twiceRem, 1)
	if twiceRem.Cmp(new(big.Int).Abs(den)) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	rounded := new(big.Rat).SetFrac(q, pow)
	return Dec{r: rounded}
}

// String renders the Dec at moneyScale fixed decimal places — the canonical form
// used to bind a money value into a parameterised SQL statement and to compare
// against a value Postgres returned. Fixed-scale so "3" and "3.000000000" compare
// equal as strings after both pass through here.
func (d Dec) String() string {
	return d.StringScale(moneyScale)
}

// StringScale renders the Dec at exactly `scale` fixed decimal places (half-up).
func (d Dec) StringScale(scale int) string {
	return d.Round(scale).rat().FloatString(scale)
}

// Equal reports exact equality of the rational values (scale-independent: 3 == 3.0).
func (d Dec) Equal(o Dec) bool {
	return d.rat().Cmp(o.rat()) == 0
}

// IsZero reports whether d is exactly zero.
func (d Dec) IsZero() bool {
	return d.rat().Sign() == 0
}

// Sign reports the sign of d: -1, 0, or +1. Used by the price loader to reject a
// negative rate (a price that would CREDIT the customer per token).
func (d Dec) Sign() int {
	return d.rat().Sign()
}
