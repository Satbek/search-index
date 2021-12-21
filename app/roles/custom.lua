local cartridge = require('cartridge')
local vshard = require('vshard')
local json = require('json')
local errors = require('errors')
local api = require('app.api')

local function init(opts) -- luacheck: no unused args
    rawset(_G, 'api', api)
    if opts.is_master then
        api.identifiers_queue.init()
    end

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd serivce")
    httpd:route({method = 'GET', path = '/hello'}, function()
        return {body = 'Hello world!'}
    end)

    local http_handler = require('metrics.plugins.prometheus').collect_http
    httpd:route({path = '/metrics'}, function(...)
        return http_handler(...)
    end)

    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    return true
end

return {
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    dependencies = {'cartridge.roles.vshard-router'},
}
