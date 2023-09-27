const {MongoClient} = require("mongodb");
const {Client4} = require('mattermost-redux/client');
const PGClient = require('pg').Client;

Client4.setUrl('http://localhost:8065');
Client4.setToken('hm6faifrjirpu89w9f7f51o5na');

const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
let database = null;

const pgClient = new PGClient({
    user: 'mmuser',
    host: 'localhost',
    database: 'mattermost_test',
    password: 'mostest',
    port: 5432,
})

async function run() {
    try {
        await pgClient.connect()
        await migrate()
    } catch (e) {
        console.error(e);
    } finally {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

run().catch(console.dir);


async function migrate() {
    await exportUsers();
    await exportChannels();
    // await deleteExportedMessages();
    await exportMessages();

    // update posts
    // set userid = props ->> 'userID',
    //     createat = NULLIF(props->>'created', '')::BIGINT,
    //     updateat = NULLIF(props->>'updated', '')::BIGINT

}

async function exportUsers() {
    const rcUsers = await getRCUserList();
    for (const rcUser of Object.values(rcUsers)) {
        let {name, username, type} = rcUser;
        if (!username) {
            if (rcUser?.emails?.length) {
                username = rcUser?.emails[0]?.address.split('@')[0];
            } else {
                console.log('no username or email, skipping', rcUser);
                continue;
            }
        }
        if (type !== 'user') {
            console.log('skipping bot ' + username);
            continue;
        }
        let email = username + '@nomail.com'
        if (rcUser?.emails?.length)
            email = rcUser?.emails[0]?.address;
        rcUser.mmdb = await updateMMUser({
            email,
            username,
            nickname: name,
        })
        // rcUser.token = await getUserToken(rcUser.mmdb.id);
        // const session = await Client4.loginById(rcUser.mmdb.id, rcUser.mmdb.username, rcUser.token.token)
        // console.log('session', session)
    }
}

async function exportChannels() {
    const channels = await getRCRoomList();
    for (const channel of Object.values(channels)) {
        let {name, fname, description} = channel
        if (!name || !/^[a-z0-9]+([a-z\-_0-9]+|(__)?)[a-z0-9]*$/.test(name)) {
            continue;
        }
        if (description && description.length > 250)
            description = description.substring(0, 250)
        channel.mmdb = await updateMMChannel({
            name,
            display_name: fname,
            ...(description && {purpose: description}),
        })
    }
}

async function deleteExportedMessages() {
    const {rowCount} = await pgClient.query("delete from posts where (props->>'imported')::boolean IS TRUE")
    console.info(`Deleted ${rowCount} posts`)

    // const channels = await getRCRoomList();
    // let deleteCount = 0;
    // for (const channel of Object.values(channels)) {
    //     if (!channel.mmdb)
    //         continue;
    //     const posts = await Client4.getPosts(channel.mmdb.id);
    //     for (const post of Object.values(posts.posts)) {
    //         if (post?.props?.rcid) {
    //             await Client4.deletePost(post.id);
    //             deleteCount++;
    //         }
    //     }
    // }
}

async function exportMessages() {
    const db = await getDatabase();
    const roomList = await getRCRoomList();
    const userList = await getRCUserList();
    const messages = db.collection('rocketchat_message');
    const messageCursor = messages.find(); //.limit(200);
    let ignoredMessages = 0;
    for await (const messageDoc of messageCursor) {
        const {_id, u, msg, rid} = messageDoc;
        const roomDoc = roomList[rid];
        if (!roomDoc || !roomDoc.mmdb)
            continue;
        if (!msg)
            continue;
        const user = userList[u._id];
        const {rowCount} = await pgClient.query("INSERT INTO posts\n" +
            "(id, channelid, userid, message, createat, updateat, props, ispinned, hasreactions, editat, deleteat, rootid, originalid, type, hashtags, filenames, fileids, remoteid) \n" +
            "values($1, $2, $3, $4, $5, $6, $7, false, false, 0, 0, '', '', '',' ', '[]', '[]', '')\n" +
            "ON CONFLICT (id) DO NOTHING",
            [_id, roomDoc.mmdb.id, user.mmdb.id, msg, messageDoc.ts.getTime(), messageDoc._updatedAt.getTime(), '{}'])
        if (rowCount === 1) {
            console.log(`<${u.username}>:`, msg);
        } else {
            ignoredMessages++;
        }
    }
    if (ignoredMessages) {
        console.log(`ignored ${ignoredMessages} messages`)
    }


}


async function getDatabase() {
    if (database === null) {
        database = client.db('parties');
    }
    return database;
}


let mmChannelList = null

async function getMMChannelList() {
    if (mmChannelList === null) {
        const team_id = await getDefaultTeamID();
        mmChannelList = await Client4.getChannels(team_id);
    }
    return mmChannelList;
}

let mmUserList = null

async function getMMUserList() {
    if (mmUserList === null) {
        mmUserList = await Client4.getProfiles();
    }
    return mmUserList;
}

let rcUserList = null;

async function getRCUserList() {
    const db = await getDatabase();
    if (rcUserList === null) {
        rcUserList = {};
        const users = db.collection('users');
        const cursor = users.find();
        for await (const doc of cursor) {
            if (doc.type !== 'bot') {
                rcUserList[doc._id] = doc;
                // console.log(doc.name || doc)
            }
        }
    }
    return rcUserList;
}

let rcRoomList = null;

async function getRCRoomList() {
    const db = await getDatabase();
    if (rcRoomList === null) {
        rcRoomList = {};
        const messages = db.collection('rocketchat_room');
        const messageCursor = messages.find();
        for await (const doc of messageCursor) {
            if (doc.name) {
                rcRoomList[doc._id] = doc;
                // console.log(doc.name || doc)
            }
        }
    }
    return rcRoomList;
}


let defaultTeamID = null;

async function getDefaultTeamID() {
    if (defaultTeamID === null) {
        const teams = await Client4.getTeams()
        // console.log('teams', teams)
        defaultTeamID = teams[0].id;
    }
    return defaultTeamID;
}

let currentUser = null;

async function getUser() {
    if (currentUser === null) {
        currentUser = await Client4.getMe();
        console.log('currentUser', currentUser)
    }
    return currentUser;
}

async function updateMMChannel(channel) {
    if (!channel.team_id) channel.team_id = await getDefaultTeamID();
    if (!channel.display_name) channel.display_name = channel.name;
    if (!channel.type) channel.type = 'O';
    const existingChannels = await getMMChannelList();
    const existingChannel = existingChannels.find(c => c.name === channel.name);
    if (existingChannel) {
        channel.id = existingChannel.id;
        // console.info("Updating channel: " + channel.name);
        return await Client4.updateChannel(channel)
    } else {
        console.info("Inserting channel: " + channel.name);
        return await Client4.createChannel(channel)
    }
}

async function getUserToken(userId, description = 'migration') {
    const tokens = await Client4.getUserAccessTokens(userId);
    for (const token of tokens) {
        if (token.description === description) {
            console.log("Revoking migration token: ", token.id);
            await Client4.revokeUserAccessToken(token.id)
        }
    }
    const token = await Client4.createUserAccessToken(userId, description);
    console.log("Created migration token: " + token.id);
    return token;
}

async function updateMMUser(user) {
    const existingUserList = await getMMUserList();
    const existingUser = existingUserList.find(u => (u.email === user.email) || (u.username === user.username.toLowerCase()));
    if (!user.password) user.password = user.email + '.pass';
    if (existingUser) {
        // user.email = existingUser.email;
        // user.id = existingUser.id;
        // console.info("Found existing user: " + user.username);
        // return await Client4.updateUser(user)
        return existingUser;
    } else {
        console.info("Inserting user: " + user.username);
        const newUser = await Client4.createUser(user)
        existingUserList.push(newUser);
        return newUser;
    }
}
