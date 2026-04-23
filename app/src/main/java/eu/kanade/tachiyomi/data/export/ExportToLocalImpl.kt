package eu.kanade.tachiyomi.data.export

import android.content.Context
import androidx.core.net.toUri
import com.hippo.unifile.UniFile
import eu.kanade.domain.entries.manga.model.toSManga
import eu.kanade.tachiyomi.data.cache.MangaCoverCache
import eu.kanade.tachiyomi.data.download.manga.MangaDownloadProvider
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.await
import eu.kanade.tachiyomi.source.online.HttpSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import logcat.LogPriority
import nl.adaptivity.xmlutil.serialization.XML
import okhttp3.Call
import okhttp3.Headers
import okhttp3.Request
import okhttp3.Response
import tachiyomi.core.common.util.system.logcat
import tachiyomi.core.metadata.comicinfo.COMIC_INFO_FILE
import tachiyomi.core.metadata.comicinfo.ComicInfo
import tachiyomi.core.metadata.comicinfo.getComicInfo
import tachiyomi.domain.entries.manga.model.Manga
import tachiyomi.domain.export.service.ExportService
import tachiyomi.domain.source.manga.service.MangaSourceManager
import tachiyomi.domain.storage.service.StorageManager
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import uy.kohesive.injekt.injectLazy
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection.HTTP_NOT_MODIFIED

class ExportToLocalImpl(
    private val context: Context,
    private val sourceManager: MangaSourceManager = Injekt.get(),
    private val downloadProvider: MangaDownloadProvider = Injekt.get(),
    private val storageManager: StorageManager = Injekt.get(),
    private val coverCache: MangaCoverCache = Injekt.get(),
    private val callFactoryLazy: Lazy<Call.Factory> = lazy { Injekt.get<NetworkHelper>().client },
    private val sourceLazy: Lazy<MangaSourceManager?> = lazy { Injekt.get<MangaSourceManager>() },
) : ExportService {


    private val xml: XML by injectLazy()

    override suspend fun exportChapter(file: UniFile, destinationSubfolder: UniFile): Result<Unit> {
        return withContext(Dispatchers.IO) {
            try {
                val destinationFile = destinationSubfolder.createFile(file.name)
                    ?: return@withContext Result.failure(Exception("Failed to create destination file"))

                context.contentResolver.openInputStream(file.uri)?.use { input ->
                    context.contentResolver.openOutputStream(destinationFile.uri)?.use { output ->
                        input.copyTo(output, bufferSize = DEFAULT_BUFFER_SIZE)
                    }
                }

                Result.success(Unit)
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to export ${file.name} to $destinationSubfolder" }
                Result.failure(e)
            }
        }
    }

    override suspend fun exportCover(manga: Manga, destinationSubfolder: UniFile): Result<Unit> {
        return withContext(Dispatchers.IO) {
            try {
                if(manga.thumbnailUrl.isNullOrBlank())
                    return@withContext Result.success(Unit)

                val coverFile = coverCache.getCoverFile(manga.thumbnailUrl)
                if (coverFile != null) {
                    val extension = manga.thumbnailUrl?.substringAfterLast(".") ?: "jpg"
                    val destinationCover = destinationSubfolder.createFile("cover.$extension")

                    val inputStream: InputStream? = when {
                        coverFile.exists() ->  context.contentResolver.openInputStream(coverFile.toUri())
                        else -> executeNetworkRequest(manga).body.byteStream()
                    }

                    inputStream?.use { input ->
                        context.contentResolver.openOutputStream(destinationCover!!.uri)?.use { output ->
                            input.copyTo(output, bufferSize = DEFAULT_BUFFER_SIZE)
                        }
                    }
                }

                Result.success(Unit)
            } catch (e: Exception) {
                logcat(LogPriority.WARN, e) { "Failed to export cover for manga ${manga.title}" }
                Result.success(Unit)
            }
        }
    }

    private suspend fun executeNetworkRequest(manga: Manga): Response {
        val httpSource: HttpSource? = (sourceLazy.value?.get(manga.source) as HttpSource?)
        val client = httpSource?.client ?: callFactoryLazy.value
        val response = client.newCall(newRequest(manga.thumbnailUrl!!, httpSource?.headers)).await()
        if (!response.isSuccessful && response.code != HTTP_NOT_MODIFIED) {
            response.close()
            throw IOException(response.message)
        }
        return response
    }

    private fun newRequest(url: String, sourceHeaders: Headers?): Request {
        val request = Request.Builder().apply {
            url(url)
            if (sourceHeaders != null) {
                headers(sourceHeaders)
            }
        }

        return request.build()
    }

    override suspend fun exportComicInfo(manga: Manga, destinationSubfolder: UniFile): Result<Unit> {
        return withContext(Dispatchers.IO) {
            try {
                val comicInfo = manga.toSManga().getComicInfo()
                destinationSubfolder.createFile(COMIC_INFO_FILE)?.openOutputStream()?.use {
                    val comicInfoString = xml.encodeToString(ComicInfo.serializer(), comicInfo)
                    it.write(comicInfoString.toByteArray())
                }
                Result.success(Unit)
            } catch (e: Exception) {
                logcat(LogPriority.WARN, e) { "Failed to export ComicInfo for manga ${manga.title}" }
                Result.success(Unit)
            }
        }
    }

    override suspend fun getItemsToExport(manga: Manga): Result<Array<UniFile>> {
        return withContext(Dispatchers.IO) {
            try {
                val mangaSource = sourceManager.get(manga.source)
                    ?: return@withContext Result.failure(Exception("Source not found"))

                val folder = downloadProvider.findMangaDir(manga.title, mangaSource)
                    ?: return@withContext Result.failure(Exception("Manga directory not found"))

                val files = folder.listFiles()
                    ?: return@withContext Result.failure(Exception("Failed to list manga files"))

                Result.success(files)
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to get export items for manga ${manga.title}" }
                Result.failure(e)
            }
        }
    }

    override suspend fun getDestinationSubfolder(manga: Manga): Result<UniFile> {
        return withContext(Dispatchers.IO) {
            try {
                val mangaSource = sourceManager.get(manga.source)
                    ?: return@withContext Result.failure(Exception("Source not found"))

                val folder = downloadProvider.findMangaDir(manga.title, mangaSource)
                    ?: return@withContext Result.failure(Exception("Manga directory not found"))

                val destinationFolder = storageManager.getLocalMangaSourceDirectory()
                    ?: return@withContext Result.failure(Exception("Local source directory not available"))

                val destination = destinationFolder.createDirectory(folder.name)
                    ?: return@withContext Result.failure(Exception("Failed to create destination directory"))

                Result.success(destination)
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to get destination subfolder for manga ${manga.title}" }
                Result.failure(e)
            }
        }
    }

    override suspend fun destinationSubfolderExists(mangaTitle: String): Result<Boolean> {
        return withContext(Dispatchers.IO) {
            try {
                val localSourceDir = storageManager.getLocalMangaSourceDirectory()
                val exists = localSourceDir?.findFile(mangaTitle) != null
                Result.success(exists)
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to check if destination subfolder exists for $mangaTitle" }
                Result.failure(e)
            }
        }
    }

    override suspend fun deleteAllItemsInSubfolder(subfolder: UniFile): Result<Unit> {
        return withContext(Dispatchers.IO) {
            try {
                subfolder.delete()
                Result.success(Unit)
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to delete subfolder $subfolder" }
                Result.failure(e)
            }
        }
    }
}
