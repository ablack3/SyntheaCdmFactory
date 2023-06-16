
#' Install SyntheaCdmFactory
#'
#' Download the necessary dependencies to create a Synthea CDM.
#' The dependencies are 1. the Synthea java program and
#' 2. the OMOP CDM vocabulary tables. These files are cached and can be
#' removed by calling `uninstall_synthea()`
#'
#' @return NULL
#' @export
#'
#' @examples
#' \dontrun{
#' install_synthea()
#' }
install_synthea <- function() {

  cli::cli_process_start("downloading synthea")
  download_synthea()
  cli::cli_process_done("downloading synthea")

  # Load vocab files into duckdb

  # cli::cli_process_start("downloading vocab")
  # download_vocab()
  # cli::cli_process_done("downloading vocab")

  # test that files are available


  # test that java works
  out <- system("java --version", intern = TRUE)
  if (!any(stringr::str_detect(tolower(out), "java"))) {
    rlang::abort("Java is not properly configured. \nSee https://solutions.posit.co/envs-pkgs/using-rjava/")
  }

  jar_path <- get_data_filepath("synthea-with-dependencies.jar")
  # download_synthea()
  stopifnot(file.exists(jar_path))

  system(glue::glue('java -jar "{get_data_filepath("synthea-with-dependencies.jar")}"'))

  rlang::check_installed("duckdb")

  # create a base cdm - not sure how to add file to cache yet.
  # pkgfilecache::ensure_files_available()
  # pkg_cache_add_file(cachepath = get_data_filepath(), file =  new_cdm_dir(file.path), relpath = dirname(file))


  # test the full generation process
  # synthesize_cdm()

  invisible(NULL)
}


# @title Download synthea jar
#
# @description Ensure that the optional data is available locally in the package cache.
# Will try to download the data only if it is not available.
#
# @return Named list. The list has entries: "available": vector of strings.
# The names of the files that are available in the local file cache.
# You can access them using get_optional_data_file(). "missing": vector of strings.
# The names of the files that this function was unable to retrieve.
download_synthea <- function() {
  pkg_info <- pkgfilecache::get_pkg_info("SyntheaCdmFactory");

  local_filenames = c("synthea-with-dependencies.jar");

  urls = c("https://github.com/synthetichealth/synthea/releases/download/v3.1.1/synthea-with-dependencies.jar")

  # md5 <filename>
  md5sums = c("7b03be6ad6bd092e940609989d147c05")

  cfiles = pkgfilecache::ensure_files_available(pkg_info, local_filenames, urls, md5sums = md5sums);
  cfiles$file_status = NULL;
  return(cfiles);
}


# download_vocab <- function() {
#   pkg_info <- pkgfilecache::get_pkg_info("SyntheaCdmFactory");
#
#   local_filenames = c("vocabulary_bundle.zip");
#
#   # get download url. Note this is stored with LFS. See https://gist.github.com/fkraeutli/66fa741d9a8c2a6a238a01d17ed0edc5
#   # curl https://api.github.com/repos/{organisation}/{repository}/contents/{file or folder path}
#   # system('curl https://api.github.com/repos/OdyOSG/SyntheaCdmFactory/contents/vocab/vocabulary_bundle_v5_0-22-JUN-22.zip')
#   # urls = c("https://media.githubusercontent.com/media/OdyOSG/SyntheaCdmFactory/main/vocab/vocabulary_bundle_v5_0-22-JUN-22.zip")
#   urls = c("https://github-cloud.githubusercontent.com/alambic/media/561076885/ce/f1/cef1a979b260b31483a3c90e4319cc42ec8345c68878c05f6ab47bc031e145da?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIMWPLRQEC4XCWWPA%2F20230312%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20230312T094103Z&X-Amz-Expires=3600&X-Amz-Signature=c66b716381a032a16e54eac2a8b8367738a3e0d1dcb5ef84d9c3292a1c9aa6a3&X-Amz-SignedHeaders=host&actor_id=0&key_id=0&repo_id=612683306&token=1")
# #https://media.githubusercontent.com/media/<username>/<repoName>/<branchName>/raw/some/path/someFile.huge
#   # https://raw.githubusercontent.com/username/repository/branch/path/filename.md
#   # "https://github.com/OdyOSG/SyntheaCdmFactory/raw/main/vocab/vocabulary_bundle_v5_0-22-JUN-22.zip");
#   # "https://api.github.com/OdyOSG/SyntheaCdmFactory/contents/vocab/vocabulary_bundle_v5_0-22-JUN-22.zip");
#   #         https://api.github.com/repos/<Project Name>/<Repository Name>/contents/<File Name>
#
#
#   download.file(urls, here::here("vocab.zip"))
#
#   # urls = "https://drive.google.com/file/d/1by7G4pLvUeepOpRqzl3ItO1WDZv_xYoK/view?usp=sharing"
#   # md5 <filename>
#   # md5sums = c("7b03be6ad6bd092e940609989d147c05")
#   md5sums = NULL
#   # "8c566a171d2f95e6890d0100df73196a");
#
#   cfiles = pkgfilecache::ensure_files_available(pkg_info, local_filenames, urls, md5sums = md5sums);
#   cfiles$file_status = NULL;
#   return(cfiles);
# }
# download_vocab()
# @title Get file names available in package cache.
#
# @description Get file names of optional data files which are available in the local package cache.
# You can access these files with get_optional_data_file().
#
# @return vector of strings. The file names available, relative to the package cache.
list_data <- function() {
  pkg_info = pkgfilecache::get_pkg_info("SyntheaCdmFactory");
  return(pkgfilecache::list_available(pkg_info));
}


# @title Access a single file from the package cache by its file name.
#
# @param filename, string. The filename of the file in the package cache.
#
# @param mustWork, logical. Whether an error should be created if the file does not exist.
# If mustWork=FALSE and the file does not exist, the empty string is returned.
#
# @return string. The full path to the file in the package cache.
# Use this in your application code to open the file.
#
get_data_filepath <- function(filename, mustWork=TRUE) {
  pkg_info = pkgfilecache::get_pkg_info("SyntheaCdmFactory");
  return(pkgfilecache::get_filepath(pkg_info, filename, mustWork=mustWork));
}


#' @title Delete all cached synthea data
#'
#' @return integer. The return value of the unlink() call: 0 for success, 1 for failure.
#' See the unlink() documentation for details.
#'
#' @export
uninstall_synthea <- function() {
  pkg_info = pkgfilecache::get_pkg_info("SyntheaCdmFactory");
  return(pkgfilecache::erase_file_cache(pkg_info));
}

