# R/utils.R

#' @keywords internal
#' @importFrom rlang %||%
`%||%` <- function(x, y) if (!is.null(x)) x else y

#' @keywords internal
.unicefData_ua <- httr::user_agent("unicefData/1.0 (+https://github.com/jpazvd/unicefData)")

#' @keywords internal
.fetch_sdmx <- function(url, ua = .unicefData_ua, retry = 3L) {
  resp <- httr::RETRY("GET", url, ua, times = retry, pause_base = 1)
  httr::stop_for_status(resp, paste("Error fetching", url))
  httr::content(resp, as = "text", encoding = "UTF-8")
}
