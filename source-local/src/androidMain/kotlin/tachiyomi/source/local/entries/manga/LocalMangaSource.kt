package tachiyomi.source.local.entries.manga

import android.content.Context
import com.hippo.unifile.UniFile
import eu.kanade.tachiyomi.source.CatalogueSource
import eu.kanade.tachiyomi.source.MangaSource
import eu.kanade.tachiyomi.source.UnmeteredSource
import eu.kanade.tachiyomi.source.model.FilterList
import eu.kanade.tachiyomi.source.model.MangasPage
import eu.kanade.tachiyomi.source.model.Page
import eu.kanade.tachiyomi.source.model.SChapter
import eu.kanade.tachiyomi.source.model.SManga
import eu.kanade.tachiyomi.util.lang.compareToCaseInsensitiveNaturalOrder
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import kotlinx.serialization.protobuf.ProtoBuf
import logcat.LogPriority
import mihon.core.archive.archiveReader
import mihon.core.archive.epubReader
import nl.adaptivity.xmlutil.core.AndroidXmlReader
import nl.adaptivity.xmlutil.serialization.XML
import tachiyomi.core.common.i18n.stringResource
import tachiyomi.core.common.storage.extension
import tachiyomi.core.common.storage.nameWithoutExtension
import tachiyomi.core.common.util.lang.withIOContext
import tachiyomi.core.common.util.system.ImageUtil
import tachiyomi.core.common.util.system.logcat
import tachiyomi.core.metadata.comicinfo.COMIC_INFO_FILE
import tachiyomi.core.metadata.comicinfo.ComicInfo
import tachiyomi.core.metadata.comicinfo.copyFromComicInfo
import tachiyomi.core.metadata.comicinfo.getComicInfo
import tachiyomi.core.metadata.tachiyomi.ChapterDetails
import tachiyomi.core.metadata.tachiyomi.MangaDetails
import tachiyomi.domain.entries.manga.model.Manga
import tachiyomi.domain.items.chapter.service.ChapterRecognition
import tachiyomi.i18n.MR
import tachiyomi.i18n.aniyomi.AYMR
import tachiyomi.source.local.entries.utils.Direction
import tachiyomi.source.local.entries.utils.Entry
import tachiyomi.source.local.entries.utils.Latest
import tachiyomi.source.local.entries.utils.None
import tachiyomi.source.local.entries.utils.PageArray
import tachiyomi.source.local.entries.utils.Pageable
import tachiyomi.source.local.entries.utils.Popular
import tachiyomi.source.local.entries.utils.Snapshot
import tachiyomi.source.local.entries.utils.UniFileLite
import tachiyomi.source.local.filter.manga.MangaOrderBy
import tachiyomi.source.local.image.manga.LocalMangaCoverManager
import tachiyomi.source.local.io.ArchiveManga
import tachiyomi.source.local.io.Format
import tachiyomi.source.local.io.manga.LocalMangaSourceFileSystem
import tachiyomi.source.local.metadata.fillMetadata
import uy.kohesive.injekt.injectLazy
import java.io.File
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.abs

actual class LocalMangaSource(
    private val context: Context,
    private val fileSystem: LocalMangaSourceFileSystem,
    private val coverManager: LocalMangaCoverManager,
) : CatalogueSource, UnmeteredSource {

    private val json: Json by injectLazy()
    private val xml: XML by injectLazy()

    @Suppress("PrivatePropertyName")
    private val PopularFilters = FilterList(MangaOrderBy.Popular(context))

    @Suppress("PrivatePropertyName")
    private val LatestFilters = FilterList(MangaOrderBy.Latest(context))

    override val name: String = context.stringResource(AYMR.strings.local_manga_source)

    override val id: Long = ID

    override val lang: String = "other"

    override fun toString() = name

    override val supportsLatest: Boolean = true

    private var snapshot = Snapshot()

    // Caches directory contents in memory to eliminate redundant disk I/O,
    // using a lightweight UniFileLite snapshot to bypass expensive system
    // calls to getName() and lastModified() during frequent access.
    private var fileCacheInMemory = AtomicReference<List<UniFileLite>?>(null)

    private val snapshotFile: File
        get() = File(context.cacheDir, "manga_local_cache_v1")

    private val memoryDumpFile: File
        get() = File(context.cacheDir, "manga_local_dump_v1")

    init {
        try {
            if (snapshotFile.exists()) {
                snapshot = snapshotFile.inputStream().use {
                    ProtoBuf.decodeFromByteArray<Snapshot>(it.readBytes())
                }
            }

            if (memoryDumpFile.exists()) {
                fileCacheInMemory.set(
                    memoryDumpFile.inputStream().use {
                        ProtoBuf.decodeFromByteArray<List<UniFileLite>>(it.readBytes())
                    },
                )
            }
        } catch (_: Exception) {
            snapshotFile.delete()
            memoryDumpFile.delete()
        }
    }

    // Browse related
    override suspend fun getPopularManga(page: Int) = getSearchManga(page, "", PopularFilters)

    override suspend fun getLatestUpdates(page: Int) = getSearchManga(page, "", LatestFilters)

    private fun updateDiskCache() {
        val bytes = ProtoBuf.encodeToByteArray(snapshot)
        try {
            snapshotFile.writeBytes(bytes)
        } catch (e: Throwable) {
            logcat(
                priority = LogPriority.ERROR,
                throwable = e,
                message = { "Failed to write disk cache file" },
            )
        }
    }

    private fun dumpMemory() {
        val bytes = ProtoBuf.encodeToByteArray(fileCacheInMemory.get())
        try {
            memoryDumpFile.writeBytes(bytes)
        } catch (e: Throwable) {
            logcat(
                priority = LogPriority.ERROR,
                throwable = e,
                message = { "Failed to write dump" },
            )
        }
    }

    override suspend fun getSearchManga(page: Int, query: String, filters: FilterList): MangasPage = withIOContext {
        val lastModifiedLimit = if (filters === LatestFilters) {
            System.currentTimeMillis() - LATEST_THRESHOLD
        } else {
            0L
        }

        if (page == 1) {
            verifyCacheTimeStamp()
        }

        val direction = when {
            filters === PopularFilters -> Popular
            filters === LatestFilters -> Latest
            else -> None
        }

        snapshot.getSortBy(direction).get(page)?.let { entryPage ->
            val mangas = entryPage.map {
                SManga.create().apply {
                    title = it.title
                    thumbnail_url = it.thumbnail
                    url = it.url
                }
            }
            return@withIOContext MangasPage(mangas, entryPage.hasNext)
        }

        val pageArray = getMangaDirPageable(filters, lastModifiedLimit, query).getPage(page)

        val mangas = pageArray
            .map { mangaDir ->
                async {
                    SManga.create().apply {
                        title = mangaDir.name
                        url = mangaDir.name

                        // Try to find the cover
                        coverManager.find(mangaDir.name)?.let {
                            thumbnail_url = it.uri.toString()
                        }
                    }
                }
            }
            .awaitAll()

        if (direction !is Direction.None) {
            saveSnapshot(direction, page, mangas, pageArray)
        }

        MangasPage(mangas, pageArray.hasNext)
    }

    private fun saveSnapshot(direction: Direction, page: Int, mangas: List<SManga>, mangaPage: PageArray) {
        val entries = mangas.map { it.toEntry() }
        snapshot.addSortBy(direction, page, entries, mangaPage.hasNext)
        updateDiskCache()
    }

    private fun verifyCacheTimeStamp() {
        val lastModified = fileSystem.getBaseDirectory()?.lastModified()
            ?: return snapshotFile.run { delete() }

        val now = System.currentTimeMillis()

        if (lastModified <= snapshot.lastModified && now < snapshot.expiration) {
            return
        }

        snapshot = Snapshot(
            lastModified = lastModified,
            expiration = now + TimeUnit.HOURS.toMillis(1),
        )
        fileCacheInMemory.set(null)
        memoryDumpFile.delete()
        updateDiskCache()
    }

    private fun getMangaDirPageable(filters: FilterList, lastModifiedLimit: Long, query: String): Pageable =
        Pageable(getMangaDir(lastModifiedLimit, query, filters))

    private fun getMangaDir(lastModifiedLimit: Long, query: String, filters: FilterList): List<UniFileLite> {
        var mangaDirs = fileCacheInMemory.get() ?: fileSystem.getFilesInBaseDirectory()
            // Filter out files that are hidden and is not a folder
            .filter { it.isDirectory && !it.name.orEmpty().startsWith('.') }
            .distinctBy { it.name }
            .let { uniFileList ->
                uniFileList.map { UniFileLite(it.name.orEmpty(), it.lastModified()) }.also {
                    fileCacheInMemory.getAndSet(it)
                    dumpMemory()
                }
            }

        mangaDirs = mangaDirs
            .filter {
                if (lastModifiedLimit == 0L && query.isBlank()) {
                    true
                } else if (lastModifiedLimit == 0L) {
                    it.name.contains(query, ignoreCase = true)
                } else {
                    it.lastModified() >= lastModifiedLimit
                }
            }

        filters.forEach { filter ->
            when (filter) {
                is MangaOrderBy.Popular -> {
                    mangaDirs = if (filter.state!!.ascending) {
                        mangaDirs.sortedWith(compareBy(String.CASE_INSENSITIVE_ORDER) { it.name })
                    } else {
                        mangaDirs.sortedWith(compareByDescending(String.CASE_INSENSITIVE_ORDER) { it.name })
                    }
                }

                is MangaOrderBy.Latest -> {
                    mangaDirs = if (filter.state!!.ascending) {
                        mangaDirs.sortedBy(UniFileLite::lastModified)
                    } else {
                        mangaDirs.sortedByDescending(UniFileLite::lastModified)
                    }
                }
                else -> {
                    /* Do nothing */
                }
            }
        }
        return mangaDirs
    }

    // Manga details related
    override suspend fun getMangaDetails(manga: SManga): SManga = withIOContext {
        coverManager.find(manga.url)?.let {
            manga.thumbnail_url = it.uri.toString()
        }

        // Augment manga details based on metadata files
        try {
            val mangaDir = fileSystem.getMangaDirectory(manga.url) ?: error("${manga.url} is not a valid directory")
            val mangaDirFiles = mangaDir.listFiles().orEmpty()

            val comicInfoFile = mangaDirFiles
                .firstOrNull { it.name == COMIC_INFO_FILE }
            val noXmlFile = mangaDirFiles
                .firstOrNull { it.name == ".noxml" }
            val legacyJsonDetailsFile = mangaDirFiles
                .firstOrNull { it.extension == "json" && it.nameWithoutExtension == "details" }

            when {
                // Top level ComicInfo.xml
                comicInfoFile != null -> {
                    noXmlFile?.delete()
                    setMangaDetailsFromComicInfoFile(comicInfoFile.openInputStream(), manga)
                }

                // Old custom JSON format
                // TODO: remove support for this entirely after a while
                legacyJsonDetailsFile != null -> {
                    json.decodeFromStream<MangaDetails>(legacyJsonDetailsFile.openInputStream()).run {
                        title?.let { manga.title = it }
                        author?.let { manga.author = it }
                        artist?.let { manga.artist = it }
                        description?.let { manga.description = it }
                        genre?.let { manga.genre = it.joinToString() }
                        status?.let { manga.status = it }
                    }
                    // Replace with ComicInfo.xml file
                    val comicInfo = manga.getComicInfo()
                    mangaDir
                        .createFile(COMIC_INFO_FILE)
                        ?.openOutputStream()
                        ?.use {
                            val comicInfoString = xml.encodeToString(ComicInfo.serializer(), comicInfo)
                            it.write(comicInfoString.toByteArray())
                            legacyJsonDetailsFile.delete()
                        }
                }

                // Copy ComicInfo.xml from chapter archive to top level if found
                noXmlFile == null -> {
                    val chapterArchives = mangaDirFiles.filter(ArchiveManga::isSupported)

                    val copiedFile = copyComicInfoFileFromArchive(chapterArchives, mangaDir)
                    if (copiedFile != null) {
                        setMangaDetailsFromComicInfoFile(copiedFile.openInputStream(), manga)
                    } else {
                        // Avoid re-scanning
                        mangaDir.createFile(".noxml")
                    }
                }
            }
        } catch (e: Throwable) {
            logcat(
                LogPriority.ERROR,
                e,
            ) { "Error setting manga details from local metadata for ${manga.title}" }
        }

        return@withIOContext manga
    }

    private fun copyComicInfoFileFromArchive(chapterArchives: List<UniFile>, folder: UniFile): UniFile? {
        for (chapter in chapterArchives) {
            chapter.archiveReader(context).use { reader ->
                reader.getInputStream(COMIC_INFO_FILE)?.use { stream ->
                    return copyComicInfoFile(stream, folder)
                }
            }
        }
        return null
    }

    private fun copyComicInfoFile(comicInfoFileStream: InputStream, folder: UniFile): UniFile? {
        return folder.createFile(COMIC_INFO_FILE)?.apply {
            openOutputStream().use { outputStream ->
                comicInfoFileStream.use { it.copyTo(outputStream) }
            }
        }
    }

    private fun setMangaDetailsFromComicInfoFile(stream: InputStream, manga: SManga) {
        val comicInfo = AndroidXmlReader(stream, StandardCharsets.UTF_8.name()).use {
            xml.decodeFromReader<ComicInfo>(it)
        }

        manga.copyFromComicInfo(comicInfo)
    }

    // Chapters
    override suspend fun getChapterList(manga: SManga): List<SChapter> = withIOContext {
        val chaptersData = fileSystem.getFilesInMangaDirectory(manga.url)
            .firstOrNull {
                it.extension == "json" && it.nameWithoutExtension == "chapters"
            }?.let { file ->
                runCatching {
                    json.decodeFromStream<List<ChapterDetails>>(file.openInputStream())
                }.getOrNull()
            }

        val chapters = fileSystem.getFilesInMangaDirectory(manga.url)
            // Only keep supported formats
            .filterNot { it.name.orEmpty().startsWith('.') }
            .filter { it.isDirectory || ArchiveManga.isSupported(it) || it.extension.equals("epub", true) }
            .map { chapterFile ->
                SChapter.create().apply {
                    url = "${manga.url}/${chapterFile.name}"
                    name = if (chapterFile.isDirectory) {
                        chapterFile.name
                    } else {
                        chapterFile.nameWithoutExtension
                    }.orEmpty()
                    date_upload = chapterFile.lastModified()

                    val chapterNumber = ChapterRecognition
                        .parseChapterNumber(manga.title, this.name, this.chapter_number.toDouble())
                        .toFloat()
                    chapter_number = chapterNumber

                    val format = Format.valueOf(chapterFile)
                    if (format is Format.Epub) {
                        format.file.epubReader(context).use { epub ->
                            epub.fillMetadata(manga, this)
                        }
                    }

                    // Overwrite data from chapters.json file
                    chaptersData?.also { dataList ->
                        dataList.firstOrNull { it.chapter_number.equalsTo(chapterNumber) }?.also { data ->
                            data.name?.also { name = it }
                            data.date_upload?.also { date_upload = parseDate(it) }
                            scanlator = data.scanlator
                        }
                    }
                }
            }
            .sortedWith { c1, c2 ->
                val c = c2.chapter_number.compareTo(c1.chapter_number)
                if (c == 0) c2.name.compareToCaseInsensitiveNaturalOrder(c1.name) else c
            }

        // Copy the cover from the first chapter found if not available
        if (manga.thumbnail_url.isNullOrBlank()) {
            chapters.lastOrNull()?.let { chapter ->
                updateCover(chapter, manga)
            }
        }

        chapters
    }

    private fun parseDate(isoDate: String): Long {
        return SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.getDefault()).parse(isoDate)?.time ?: 0L
    }

    private fun Float.equalsTo(other: Float): Boolean {
        return abs(this - other) < 0.0001
    }

    // Filters
    override fun getFilterList() = FilterList(MangaOrderBy.Popular(context))

    // Unused stuff
    override suspend fun getPageList(chapter: SChapter): List<Page> = throw UnsupportedOperationException(
        "Unused",
    )

    fun getFormat(chapter: SChapter): Format {
        try {
            val (mangaDirName, chapterName) = chapter.url.split('/', limit = 2)
            return fileSystem.getBaseDirectory()
                ?.findFile(mangaDirName)
                ?.findFile(chapterName)
                ?.let(Format.Companion::valueOf)
                ?: throw Exception(context.stringResource(MR.strings.chapter_not_found))
        } catch (e: Format.UnknownFormatException) {
            throw Exception(context.stringResource(MR.strings.local_invalid_format))
        } catch (e: Exception) {
            throw e
        }
    }

    private fun updateCover(chapter: SChapter, manga: SManga): UniFile? {
        return try {
            when (val format = getFormat(chapter)) {
                is Format.Directory -> {
                    val entry = format.file.listFiles()
                        ?.sortedWith { f1, f2 ->
                            f1.name.orEmpty().compareToCaseInsensitiveNaturalOrder(
                                f2.name.orEmpty(),
                            )
                        }
                        ?.find {
                            !it.isDirectory && ImageUtil.isImage(it.name) { it.openInputStream() }
                        }

                    entry?.let { coverManager.update(manga, it.openInputStream()) }
                }
                is Format.Archive -> {
                    format.file.archiveReader(context).use { reader ->
                        val entry = reader.useEntries { entries ->
                            entries
                                .sortedWith { f1, f2 -> f1.name.compareToCaseInsensitiveNaturalOrder(f2.name) }
                                .find { it.isFile && ImageUtil.isImage(it.name) { reader.getInputStream(it.name)!! } }
                        }

                        entry?.let { coverManager.update(manga, reader.getInputStream(it.name)!!) }
                    }
                }
                is Format.Epub -> {
                    format.file.epubReader(context).use { epub ->
                        val entry = epub.getImagesFromPages().firstOrNull()

                        entry?.let { coverManager.update(manga, epub.getInputStream(it)!!) }
                    }
                }
            }
        } catch (e: Throwable) {
            logcat(LogPriority.ERROR, e) { "Error updating cover for ${manga.title}" }
            null
        }
    }

    companion object {
        const val ID = 0L
        const val HELP_URL = "https://aniyomi.org/help/guides/local-manga/"
        private val LATEST_THRESHOLD = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS)
    }
}

fun Manga.isLocal(): Boolean = source == LocalMangaSource.ID

fun MangaSource.isLocal(): Boolean = id == LocalMangaSource.ID

fun SManga.toEntry() = Entry(
    title = title,
    thumbnail = thumbnail_url,
    url = url,
)
