local M = {}
local errors = require('errors')
local json = require('json')
local log = require('log')
local fiber = require('fiber')

M.user_not_exists_err = errors.new_class('user_not_exists')
M.replace_err = errors.new_class('replace_user')

local function get_user_tuple_by_id(id)
    return box.space.user:get({id})
end

local function user_tuple_to_map(t)
    local map = t:tomap({names_only = true})
    return map
end

local function user_tuple_to_output(t)
    local map = t:tomap({names_only = true})
    map.bucket_id = nil
    return map
end

local function user_map_to_tuple(map)
    local user_t, err = box.space.user:frommap(map)
    if user_t == nil then
        return nil, err
    end
    return user_t
end

function M.get_user_by_id(id)
    local user_t = get_user_tuple_by_id(id)
    if user_t == nil then
        return nil, M.user_not_exists_err:new('id=%s', id)
    end
    return user_tuple_to_output(user_t)
end


function M.add_user(user_data)
    local user_t, err = user_map_to_tuple(user_data)
    if user_t == nil then
        return nil, M.replace_err:new('id=%s,err=%s', user_data.id, err)
    end
    log.info("replace user: %s", user_data.id)
    box.space.user:insert(user_t)
end

function M.get_users_by_name(user_name)
    local yield_every = 100
    local count = 1

    local result = {}
    for _, t in box.space.user.index.name:pairs({user_name}, 'EQ') do
        if count % yield_every == 0 then
            fiber.yield()
        end

        count = count + 1
        table.insert(result, user_tuple_to_output(t))
    end

    return result
end

function M.change_phone_number(id, new_phone_number)
    local user_t = get_user_tuple_by_id(id)
    if user_t == nil then
        return nil, M.user_not_exists_err:new('id=%s', id)
    end

    local user_map = user_tuple_to_map(user_t)
    local old_phone_number = user_map.phone_number

    user_map.phone_number = new_phone_number
    local new_user_t, err = user_map_to_tuple(user_map)
    if new_user_t == nil then
        return nil, M.replace_err:new('id=%s,err=%s',id,err)
    end

    box.space.user:replace(new_user_t)

    return old_phone_number, err
end

return M
