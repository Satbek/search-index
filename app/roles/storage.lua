local cartridge = require('cartridge')
local storage = require('app.storage')
local search_storage_api = require('app.search_storage_api')

local function init(opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd serivce")
    httpd:route({method = 'GET', path = '/hello'}, function()
        return {body = 'Hello world!'}
    end)

    local http_handler = require('metrics.plugins.prometheus').collect_http
    httpd:route({path = '/metrics'}, function(...)
        return http_handler(...)
    end)

    rawset(_G, 'storage_api', storage)
    rawset(_G, 'search_storage_api', search_storage_api)
    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    if opts.is_master then
        box.schema.create_space('user', {
            if_not_exists = true,
            format = {
                {name = 'id', type = 'string'}, --uuid,
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'name', type = 'string'},
                {name = 'phone_number', type = 'string', is_nullable = true},
                {name = 'email', type = 'string', is_nullable = true},
                {name = 'birthdate', type = 'unsigned', is_nullable = true},
                {name = 'passport_num', type = 'string', is_nullable = true},
                {name = 'metadata', type = 'any', is_nullable = true},
            }
        })
        box.space.user:create_index('pk', {
            if_not_exists = true,
            unique = true,
            parts = {
                {'id', 'string'},
            }
        })

        box.space.user:create_index('name', {
            if_not_exists = true,
            unique = false,
            parts = {
                {'name', 'string'},
            }
        })

        box.space.user:create_index('bucket_id', {
            if_not_exists = true,
            unique = false,
            parts = {
                {'bucket_id', 'unsigned'},
            }
        })

        box.schema.create_space('user_search_index', {
            if_not_exists = true,
            format = {
                {name = 'user_id', type = 'string'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'data_hash', type = 'string'},
                {name = 'data', type = 'any'},
            },
        })

        box.space.user_search_index:create_index('pk', {
            if_not_exists = true,
            unique = true,
            parts = {
                {'user_id', 'string'},
                {'data_hash', 'string'},
            }
        })

        box.space.user_search_index:create_index('hash', {
            if_not_exists = true,
            unique = false,
            parts = {
                {'data_hash', 'string'},
            }
        })
    end

    return true
end

return {
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    dependencies = {'cartridge.roles.vshard-storage'},
}
