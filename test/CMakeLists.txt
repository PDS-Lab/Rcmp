function(add_test TEST_FILE)
  get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
  add_executable(${TEST_NAME} ${TEST_FILE})
  target_link_libraries(${TEST_NAME}
  rcmp
  redis++
  hiredis
  )
  message(STATUS "[ADD TEST] : ${TEST_NAME}")
endfunction()

file(GLOB_RECURSE test_files *.cc)

foreach(test_file ${test_files})
  add_test(${test_file})
endforeach(test_file)
