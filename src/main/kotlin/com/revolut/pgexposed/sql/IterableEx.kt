package com.revolut.pgexposed.sql

interface SizedIterable<out T>: Iterable<T> {
    fun limit(n: Int, offset: Int = 0): SizedIterable<T>
    fun count(): Int
    fun empty(): Boolean
    fun forUpdate(): SizedIterable<T> = this
    fun notForUpdate(): SizedIterable<T> = this
    fun copy() : SizedIterable<T>
    fun orderBy(vararg order: Pair<Expression<*>, SortOrder>) : SizedIterable<T>
}
