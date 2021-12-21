local cartridge_rpc = require('cartridge.rpc')
local cartridge_pool = require('cartridge.pool')

local vshard = require('vshard')
local fiber = require('fiber')
local log = require('log')
local search_index = require('app.search_api')
local json = require('json')
local confapplier = require('cartridge.confapplier')

local end_ch = fiber.channel()

local success_state = 'RolesConfigured'
local function config_successfully_applied()
    return success_state == confapplier.wish_state(success_state, math.huge)
end


local function init(opts) -- luacheck: no unused args
    local sleep_time = 0.1
    fiber.create(function()
        fiber.self():name('search-worker')
        log.info("[search-worker] start")
        config_successfully_applied()
        log.info("wait config_successfully_applied")

        while true do
            fiber.sleep(sleep_time)
            if end_ch:is_closed() then
                log.info("[search-worker] stop")
            end

            local conn, err = cartridge_rpc.get_connection('app.roles.custom', {leader_only = true})
            if err ~= nil then
                log.error(("[search-worker] get_random_api_conn failed,err=%s"):format(err))
                goto continue
            end

            local task, err = conn:eval('return queue.tube.identifiers_tube:take()')
            if err ~= nil then
                log.error("[search-worker] queue.tube.identifiers_tube:take() failed")
                goto continue
            end

            log.info("[search-worker] take task,id=%s,task_data=%s", task[1], json.encode(task[3]))
            local task_data = task[3]
            --todo get rid of if/else
            if task_data.identifier_name == 'phone_number' then
                if task_data.operation == 'add' then
                    local _
                    _, err = search_index.user_id.add_phone_number_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] add_phone_number_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                elseif task_data.operation == 'delete' then
                    local _
                    _, err = search_index.user_id.delete_phone_number_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] delete_phone_number_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                end
            end

            if task_data.identifier_name == 'phone_number_hash' then
                if task_data.operation == 'add' then
                    local _
                    _, err = search_index.user_id.add_phone_number_hash_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] add_phone_number_hash_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                elseif task_data.operation == 'delete' then
                    local _
                    _, err = search_index.user_id.delete_phone_number_hash_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] delete_phone_number_hash_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                end
            end

            if task_data.identifier_name == 'email' then
                if task_data.operation == 'add' then
                    local _
                    _, err = search_index.user_id.add_email_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] add_email_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                end
            end

            if task_data.identifier_name == 'passport_num' then
                if task_data.operation == 'add' then
                    local _
                    _, err = search_index.user_id.add_passport_num_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] add_passport_num_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                end
            end

            if task_data.identifier_name == 'metadata_geo' then
                if task_data.operation == 'add' then
                    local _
                    _, err = search_index.user_id.add_geoposition_identifier(task_data.user_id, task_data.data)
                    if err ~= nil then
                        log.error(("[search-worker] add_geoposition_identifier fail,id=%s,err=%s"):format(task[1],err))
                        conn:eval(('return queue.tube.identifiers_tube:release(%s)'):format(task[1]))
                    end
                end
            end

            local _, err = conn:eval(('return queue.tube.identifiers_tube:ack(%s)'):format(task[1]))
            if err ~= nil then
                log.error(("[search-worker] failed to ack,id=%s"):format(task[1]))
            end
            log.info("[search-worker] ack task,id=%s", task[1])
            ::continue::
        end
    end)

    return true
end

local function stop()
    end_ch:close()
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
