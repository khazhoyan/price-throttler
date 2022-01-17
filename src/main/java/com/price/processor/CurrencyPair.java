package com.price.processor;

import java.util.Currency;

import static java.util.Objects.requireNonNull;

public final class CurrencyPair {

    public final Currency from;
    public final Currency to;

    public CurrencyPair(Currency from, Currency to) {
        this.from = requireNonNull(from);
        this.to = requireNonNull(to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CurrencyPair ccyPair = (CurrencyPair) o;

        if (!from.equals(ccyPair.from)) return false;
        return to.equals(ccyPair.to);
    }

    @Override
    public int hashCode() {
        int result = from.hashCode();
        result = 31 * result + to.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return from.getCurrencyCode() + to.getCurrencyCode();
    }
}
