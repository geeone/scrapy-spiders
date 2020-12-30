script = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    formdata=splash.args.formdata
    })
  assert(splash:wait(0.5))

  -- don't crop image by a viewport
  splash:set_viewport_full()

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
    png = splash:png(),
  }
end
"""

autos_detran_ro_script = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    formdata=splash.args.formdata
    })

  local button = splash:select('#BotaoIntegral')
  if button then
    button:mouse_click()
    assert(splash:wait(5))
  end

  -- don't crop image by a viewport
  splash:set_viewport_full()

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
    png = splash:png(),
  }
end
"""

script_10_sec_wait = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    formdata=splash.args.formdata
    })
  assert(splash:wait(10))

  -- don't crop image by a viewport
  splash:set_viewport_full()

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
    png = splash:png(),
  }
end
"""

script_30_sec_wait = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    formdata=splash.args.formdata
    })
  assert(splash:wait(30))

  -- don't crop image by a viewport
  splash:set_viewport_full()

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
    png = splash:png(),
  }
end
"""

script_60_sec_wait = """
function main(splash)
  splash:init_cookies(splash.args.cookies)
  assert(splash:go{
    splash.args.url,
    headers=splash.args.headers,
    http_method=splash.args.http_method,
    body=splash.args.body,
    formdata=splash.args.formdata
    })
  assert(splash:wait(60))

  -- don't crop image by a viewport
  splash:set_viewport_full()

  local entries = splash:history()
  local last_response = entries[#entries].response
  return {
    url = splash:url(),
    headers = last_response.headers,
    http_status = last_response.status,
    cookies = splash:get_cookies(),
    html = splash:html(),
    png = splash:png(),
  }
end
"""
