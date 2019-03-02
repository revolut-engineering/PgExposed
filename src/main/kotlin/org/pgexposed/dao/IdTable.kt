package org.pgexposed.dao

import org.pgexposed.sql.Column
import org.pgexposed.sql.Table

abstract class IdTable<T:Comparable<T>>(name: String=""): Table(name) {
    abstract val id : Column<EntityID<T>>

}
