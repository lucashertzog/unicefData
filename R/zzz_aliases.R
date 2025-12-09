# =============================================================================
# zzz_aliases.R - Lowercase aliases for cross-platform consistency
# =============================================================================
# 
# This file provides lowercase aliases for the main functions to ensure
# consistency with Stata's case-insensitive command syntax.
#
# Usage:
#   R/Python: unicefdata() or unicefData() - both work
#   Stata:    unicefdata - case insensitive
# =============================================================================

#' @title Fetch UNICEF Indicator Data (lowercase alias)
#' @description Lowercase alias for \code{\link{unicefData}} for consistency with Stata.
#' @inheritParams unicefData
#' @return A tibble (data.frame) containing the requested data.
#' @seealso \code{\link{unicefData}}
#' @examples
#' \dontrun{
#' # These are equivalent:
#' df <- unicefData(indicator = "CME_MRY0T4", countries = "AFG")
#' df <- unicefdata(indicator = "CME_MRY0T4", countries = "AFG")
#' }
#' @export
unicefdata <- unicefData

#' @title Fetch Raw UNICEF Data (lowercase alias)
#' @description Lowercase alias for \code{\link{unicefData_raw}} for consistency with Stata.
#' @inheritParams unicefData_raw
#' @return A tibble (data.frame) containing raw API data.
#' @seealso \code{\link{unicefData_raw}}
#' @examples
#' \dontrun{
#' # These are equivalent:
#' df <- unicefData_raw(indicator = "CME_MRY0T4")
#' df <- unicefdata_raw(indicator = "CME_MRY0T4")
#' }
#' @export
unicefdata_raw <- unicefData_raw
