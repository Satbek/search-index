local t = require('luatest')
local g = t.group('integration_api')
local uuid = require('uuid')

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
    helper.truncate_space_on_cluster(g.cluster, 'user')
    helper.truncate_space_on_cluster(g.cluster, 'user_search_index')
end)


g.test_sample = function()
    local server = cluster.main_server
    local response = server:http_request('post', '/admin/api', {json = {query = '{}'}})
    t.assert_equals(response.json, {data = {}})
    t.assert_equals(server.net_box:eval('return box.cfg.memtx_dir'), server.workdir)
end

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
    local _, err = server.net_box:call('api.replace_user', {user.id, user})
    t.assert_equals(err, nil)

    local actual_user, err = server.net_box:call('api.get_user_by_id', {user.id})
    t.assert_equals(err, nil)
    t.assert_equals(actual_user, user)
end

g.test_find_user_by_name = function()
    local server = cluster.main_server
    local user = create_test_user()
    local _, err = server.net_box:call('api.replace_user', {user.id, user})
    t.assert_equals(err, nil)

    for _ = 1, 100 do
        local another_user = create_test_user()
        local _, err = server.net_box:call('api.replace_user', {another_user.id, another_user})
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

local function replace_users(users)
    local server = cluster.main_server

    for _, u in pairs(users) do
        local _, err = server.net_box:call('api.replace_user', {u.id, u})
        t.assert_equals(err, nil)
    end
end

g.test_find_users_by_name = function()
    local search_name = 'first'
    local users_to_search = create_users_with_same_name(search_name, 10)
    local another_users = create_users_with_same_name('second', 100)
    replace_users(users_to_search)
    replace_users(another_users)

    local actual_users, err = cluster.main_server.net_box:call('api.find_users_by_name', {search_name})
    t.assert_equals(err, nil)
    t.assert_items_equals(actual_users, users_to_search)
end

local function create_full_test_user()
    local user = {
        id = uuid.str(),
        name = 'full',
        phone_number = tostring(math.random(100000, 200000)),
        email = 'example@example.com',
        birthdate = 721208397,
        passport_num = '12345',
        metadata = {
            geo = {
                longitude = 54.572062,
                latitude = 40.170847,
            },
            brands = {
                Nike = true,
                Adidas = true,
                ["McDonald’s"] = true,
            }
        },
    }
    return user
end

g.test_replace_full_user = function()
    local user = create_full_test_user()
    local _, err = g.cluster.main_server.net_box:call('api.replace_user', {user.id, user})
    t.assert_equals(err, nil)

    local actual_user, err = g.cluster.main_server.net_box:call('api.get_user_by_id', {user.id})
    t.assert_equals(err, nil)
    t.assert_equals(actual_user, user)
end

g.test_find_user_by_phone_number = function()
    local user = create_full_test_user()
    local _, err = g.cluster.main_server.net_box:call('api.replace_user', {user.id, user})
    t.assert_equals(err, nil)

    local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_phone_number', {user.phone_number})
    t.assert_equals(err, nil)
    t.assert_items_include(actual_users, { user })
end

g.test_find_users_by_phone_numbers = function()
    local phone_number = '888888888'
    local user_a = create_full_test_user()
    local user_b = create_full_test_user()
    user_a.phone_number = phone_number
    user_b.phone_number = phone_number

    local _, err = g.cluster.main_server.net_box:call('api.replace_user', {user_a.id, user_a})
    t.assert_equals(err, nil)

    local _, err = g.cluster.main_server.net_box:call('api.replace_user', {user_b.id, user_b})
    t.assert_equals(err, nil)

    local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_phone_number', {phone_number})
    t.assert_equals(err, nil)
    t.assert_items_include(actual_users, { user_a, user_b })
end

g.test_find_user_by_email = function()
    local email = 'email@email.com'
    local user = create_full_test_user()
    user.email = email
    local _, err = g.cluster.main_server.net_box:call('api.replace_user', {user.id, user})
    t.assert_equals(err, nil)

    local actual_users, err = g.cluster.main_server.net_box:call('api.find_users_by_email', {email})
    t.assert_equals(err, nil)
    t.assert_items_include(actual_users, { user })
end
