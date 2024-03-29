local t = require('luatest')
local g = t.group('integration_api')
local uuid = require('uuid')
local digest = require('digest')

local helper = require('test.helper')
local cluster = helper.cluster


g.before_all(function()
    g.cluster = helper.cluster
    g.cluster:start()
end)

g.after_all(function()
    helper.stop_cluster(g.cluster)
end)

g.after_each(function()
    helper.truncate_space_on_cluster(g.cluster, 'user_search_index')
end)


g.test_metrics = function()
    local server = cluster.main_server
    local response = server:http_request('get', '/metrics')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.reason, "Ok")
end

local function create_test_user()
    local id = uuid.str()
    local data = {
        name = 'test ' .. math.random(100000),
        id = id,
    }
    return data
end

g.test_get_user_by_id = function()
    local server = cluster.main_server
    local user = create_test_user()
    local _, err = server.net_box:call('api.add_user', {user.id, user})
    t.assert_equals(err, nil)

    local actual_user, err = server.net_box:call('api.get_user_by_id', {user.id})
    t.assert_equals(err, nil)
    t.assert_equals(actual_user, user)
end

g.test_find_user_by_name = function()
    local server = cluster.main_server
    local user = create_test_user()
    local _, err = server.net_box:call('api.add_user', {user.id, user})
    t.assert_equals(err, nil)

    for _ = 1, 100 do
        local another_user = create_test_user()
        local _, err = server.net_box:call('api.add_user', {another_user.id, another_user})
        t.assert_equals(err, nil)
    end

    local actual_users, err = server.net_box:call('api.find_users_by_name', {user.name})
    t.assert_equals(err, nil)
    t.assert_items_equals(actual_users, {user})
end

local function create_users_with_same_name(name, count)
    local users = {}
    for i = 1, count do
        users[i] = create_test_user()
        users[i].name = name
    end
    return users
end

local function add_users(users)
    local server = cluster.main_server

    for _, u in pairs(users) do
        local _, err = server.net_box:call('api.add_user', {u.id, u})
        t.assert_equals(err, nil)
    end
end

g.test_find_users_by_name = function()
    local search_name = 'first'
    local users_to_search = create_users_with_same_name(search_name, 10)
    local another_users = create_users_with_same_name('second', 100)
    add_users(users_to_search)
    add_users(another_users)

    local actual_users, err = cluster.main_server.net_box:call('api.find_users_by_name', {search_name})
    t.assert_equals(err, nil)
    t.assert_items_equals(actual_users, users_to_search)
end

local function create_full_test_user()
    local user = {
        id = uuid.str(),
        name = 'full',
        birthdate = math.random(100000, 200000),
        phone_number = tostring(math.random(100000, 200000)),
    }
    return user
end

g.test_replace_full_user = function()
    local user = create_full_test_user()
    local _, err = g.cluster.main_server.net_box:call('api.add_user', {user.id, user})
    t.assert_equals(err, nil)

    local actual_user, err = g.cluster.main_server.net_box:call('api.get_user_by_id', {user.id})
    t.assert_equals(err, nil)
    t.assert_equals(actual_user, user)
end

g.test_find_user_by_phone_number = function()
    local user = create_full_test_user()
    local _, err = g.cluster.main_server.net_box:call('api.add_user', {user.id, user})
    t.assert_equals(err, nil)

    t.helpers.retrying({timeout = 10}, function()
        local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_phone_number', {user.phone_number})
        t.assert_equals(err, nil)
        t.assert_items_include(actual_users, { user })
    end)
end

g.test_find_users_by_phone_numbers = function()
    local phone_number = '888888888'
    local user_a = create_full_test_user()
    local user_b = create_full_test_user()
    user_a.phone_number = phone_number
    user_b.phone_number = phone_number

    local _, err = g.cluster.main_server.net_box:call('api.add_user', {user_a.id, user_a})
    t.assert_equals(err, nil)

    local _, err = g.cluster.main_server.net_box:call('api.add_user', {user_b.id, user_b})
    t.assert_equals(err, nil)

    t.helpers.retrying({timeout = 10}, function()
        local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_phone_number', {phone_number})
        t.assert_equals(err, nil)
        t.assert_items_include(actual_users, { user_a, user_b })
    end)
end

g.test_change_phone_number = function()
    local user = create_full_test_user()

    local _, err = g.cluster.main_server.net_box:call('api.add_user', {user.id, user})
    t.assert_equals(err, nil)

    local new_phone_number = '88888888'
    local old_phone_number = user.phone_number

    user.phone_number = new_phone_number
    local _, err = g.cluster.main_server.net_box:call('api.change_phone_number', {user.id, new_phone_number})
    t.assert_equals(err, nil)

    local actual_user, err = g.cluster.main_server.net_box:call('api.get_user_by_id', {user.id})
    t.assert_equals(err, nil)
    t.assert_equals(actual_user, user)

    t.helpers.retrying({timeout = 10}, function()
        local _, err = g.cluster.main_server.net_box:call('api.find_users_by_phone_number', {old_phone_number})
        t.assert_equals(err.class_name, "SEARCH_STORAGE: NOT_FOUND")

        local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_phone_number', {new_phone_number})
        t.assert_equals(err, nil)
        t.assert_items_include(actual_users, { user })
    end)
end

g.test_find_user_by_name_birthdate = function()
    local user = create_full_test_user()
    local _, err = g.cluster.main_server.net_box:call('api.add_user', {user.id, user})
    t.assert_equals(err, nil)

    t.helpers.retrying({timeout = 10}, function()
        local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_name_birthdate', {user.name, user.birthdate})
        t.assert_equals(err, nil)
        t.assert_items_include(actual_users, { user })
    end)
end
