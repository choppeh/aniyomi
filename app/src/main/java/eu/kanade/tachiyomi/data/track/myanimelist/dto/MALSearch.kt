package eu.kanade.tachiyomi.data.track.myanimelist.dto

import kotlinx.serialization.Serializable

@Serializable
data class MALSearchResult<T>(
    val data: List<MALSearchResultNode<T>>,
    val paging: MALSearchPaging,
)

@Serializable
data class MALSearchResultNode<T>(
    val node: T,
)

@Serializable
data class MALSearchPaging(
    val next: String? = null,
)
