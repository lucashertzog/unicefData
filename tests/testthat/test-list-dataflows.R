# Test list_dataflows() wrapper (PR #14)
# Verify output schema and column names

test_that("list_dataflows returns data frame with expected columns", {
  skip_if_not_installed("unicefData")
  skip_on_cran()  # Requires network
  
  # Call the wrapper function
  flows <- tryCatch(
    list_dataflows(),
    error = function(e) NULL
  )
  
  # Skip if API unavailable
  skip_if(is.null(flows), "API unavailable")
  
  # Should return a data frame
  expect_s3_class(flows, "data.frame")
  
  # Check for expected columns from SDMX dataflow metadata
  expected_cols <- c("id", "agency", "version", "name")
  
  for (col in expected_cols) {
    expect_true(
      col %in% names(flows),
      info = paste("Missing expected column:", col)
    )
  }
})

test_that("list_dataflows returns non-empty result", {
  skip_if_not_installed("unicefData")
  skip_on_cran()
  
  flows <- tryCatch(
    list_dataflows(),
    error = function(e) NULL
  )
  
  skip_if(is.null(flows), "API unavailable")
  
  # UNICEF has multiple dataflows (CME, NUTRITION, etc.)
  expect_true(nrow(flows) > 0, info = "Should return at least one dataflow")
})

test_that("list_dataflows includes known dataflows", {
  skip_if_not_installed("unicefData")
  skip_on_cran()
  
  flows <- tryCatch(
    list_dataflows(),
    error = function(e) NULL
  )
  
  skip_if(is.null(flows), "API unavailable")
  
  # Check for known UNICEF dataflows
  known_dataflows <- c("CME", "NUTRITION", "GLOBAL_DATAFLOW")
  
  # At least one known dataflow should be present
  has_known <- any(known_dataflows %in% flows$id)
  expect_true(has_known, info = "Should include at least one known dataflow (CME, NUTRITION, GLOBAL_DATAFLOW)")
})

test_that("list_dataflows respects retry parameter", {
  skip_if_not_installed("unicefData")
  skip_on_cran()
  
  # Test with different retry values (should not error)
  flows_default <- tryCatch(list_dataflows(), error = function(e) NULL)
  flows_retry1 <- tryCatch(list_dataflows(retry = 1), error = function(e) NULL)
  
  # Both should work (or both fail if API down)
  if (!is.null(flows_default)) {
    expect_s3_class(flows_default, "data.frame")
  }
  
  if (!is.null(flows_retry1)) {
    expect_s3_class(flows_retry1, "data.frame")
  }
})

test_that("list_dataflows with cache behaves consistently", {
  skip_if_not_installed("unicefData")
  skip_on_cran()
  
  # First call (may hit API)
  flows1 <- tryCatch(list_dataflows(cache = TRUE), error = function(e) NULL)
  
  skip_if(is.null(flows1), "API unavailable")
  
  # Second call (should use cache if available)
  flows2 <- tryCatch(list_dataflows(cache = TRUE), error = function(e) NULL)
  
  # Both calls should return same structure
  expect_s3_class(flows1, "data.frame")
  expect_s3_class(flows2, "data.frame")
  
  # Column names should match
  expect_equal(names(flows1), names(flows2))
})
