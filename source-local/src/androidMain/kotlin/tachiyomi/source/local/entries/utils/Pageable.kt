package tachiyomi.source.local.entries.utils

import com.hippo.unifile.UniFile

class Pageable {
    private var pages: List<PageArray>

    constructor(values: List<UniFile>) {
        val chunked = values.chunked(PAGE_SIZE)
        pages = chunked.mapIndexed { index, value ->
            PageArray(index, value, hasNext = chunked.lastIndex != index)
        }
    }
    fun getPage(page: Int) =
        pages.getOrElse(page - 1) { PageArray(page, emptyList(), false) }

    companion object {
        const val PAGE_SIZE = 15
    }
}

class PageArray(
    private val index: Int,
    private val value: List<UniFile>,
    val hasNext: Boolean = false,
) {
    fun <T> map(transformer: (UniFile) -> T) = value.map(transformer)
}
