package tachiyomi.source.local.entries.utils

import kotlinx.serialization.Serializable

class Pageable {
    private var pages: List<PageArray>

    constructor(values: List<UniFileLite>) {
        val chunked = values.chunked(PAGE_SIZE)
        pages = chunked.mapIndexed { index, value ->
            PageArray(value, hasNext = chunked.lastIndex != index)
        }
    }
    fun getPage(page: Int) =
        pages.getOrElse(page - 1) { PageArray(emptyList(), false) }

    companion object {
        const val PAGE_SIZE = 15
    }
}

class PageArray(
    private val value: List<UniFileLite>,
    val hasNext: Boolean = false,
) {
    fun <T> map(transformer: (UniFileLite) -> T) = value.map(transformer)
}

@Serializable
class UniFileLite(
    val name: String,
    private val lastModified: Long,
) {
    fun lastModified() = lastModified
}
