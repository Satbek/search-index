local cartridge = require('cartridge')
local storage = require('app.storage')

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

        box.space.user:create_index('bucket_id', {
            if_not_exists = true,
            unique = false,
            parts = {
                {'bucket_id', 'unsigned'},
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
