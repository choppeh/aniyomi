package tachiyomi.source.local.entries.utils

import kotlinx.serialization.Serializable

sealed class Direction {
    class Latest : Direction()
    class Popular : Direction()
    class None : Direction()
}

val Latest = Direction.Latest()
val Popular = Direction.Popular()
val None = Direction.None()

@Serializable
class Snapshot(
    val latest: EntryPageable = EntryPageable(),
    val popular: EntryPageable = EntryPageable(),
    var lastModified: Long = 0,
    var expiration: Long = 0,
) {
    fun getSortBy(direction: Direction): EntryPageable {
        return when (direction) {
            is Direction.Popular -> popular
            is Direction.Latest -> latest
            else -> EntryPageable.empty()
        }
    }

    fun addSortBy(direction: Direction, page: Int, entries: List<Entry>, hasNext: Boolean) {
        getSortBy(direction).apply {
            add(page, entries, hasNext)
        }
    }
}

@Serializable
class Entry(
    val title: String,
    val url: String,
    val thumbnail: String?,
)

@Serializable
class EntryPage(
    val page: Int,
    private val entries: List<Entry>,
    val hasNext: Boolean,
) {
    fun <T> map(transform: (Entry) -> T) = entries.map(transform)
}

@Serializable
class EntryPageable(
    val pages: MutableMap<Int, EntryPage> = mutableMapOf(),
) {
    fun add(page: Int, entries: List<Entry>, hasNext: Boolean) {
        pages[page] = EntryPage(page, entries, hasNext)
    }

    fun get(page: Int) = pages[page]

    companion object {
        fun empty() = EntryPageable()
    }
}
