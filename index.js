const {MongoClient} = require("mongodb");
const {Client4} = require('mattermost-redux/client');

Client4.setUrl('http://localhost:8065');
Client4.setToken('hm6faifrjirpu89w9f7f51o5na');

const uri = "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
let database = null;


async function run() {
    try {
        await migrate()
    } finally {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

run().catch(console.dir);


async function migrate() {
    await exportChannels();
    await deleteExportedMessages();
    await exportMessages();
}

async function exportChannels() {
    // Client4.getPosts()


    const channels = await getRCRoomList();
    for (const channel of Object.values(channels)) {
        let {name, fname, description} = channel
        if (!name || !/^[a-z0-9]+([a-z\-_0-9]+|(__)?)[a-z0-9]*$/.test(name)) {
            continue;
        }
        if (description && description.length > 250)
            description = description.substring(0, 250)
        channel.mmdb = await updateMMChannel(channel.name, {
            name,
            display_name: fname,
            ...(description && {purpose: description}),
        })
    }
}

async function deleteExportedMessages() {
    const channels = await getRCRoomList();
    let deleteCount = 0;
    for (const channel of Object.values(channels)) {
        if (!channel.mmdb)
            continue;
        const posts = await Client4.getPosts(channel.mmdb.id);
        for (const post of Object.values(posts.posts)) {
            if (post?.props?.rcid) {
                await Client4.deletePost(post.id);
                deleteCount++;
            }
        }
    }
    console.info(`Deleted ${deleteCount} posts`)
}

async function exportMessages() {
    const db = await getDatabase();
    const roomList = await getRCRoomList();
    const messages = db.collection('rocketchat_message');
    const messageCursor = messages.find(); //.limit(200);
    for await (const messageDoc of messageCursor) {
        const {u, msg, rid} = messageDoc;
        const roomDoc = roomList[rid];
        if (!roomDoc || !roomDoc.mmdb)
            continue;
        if (!msg)
            continue;
        console.log("Inserting message: ", u.username, msg);
        await Client4.createPost({
            channel_id: roomDoc.mmdb.id,
            message: `${u.username}: ${msg}`,
            props: {
                rcid: roomDoc._id,
                // user: messageDoc.u.username
            }
        })
    }
}


async function getDatabase() {
    if (database === null) {
        database = client.db('parties');
    }
    return database;
}


let existingChannels = null

async function getMMChannelList() {
    if (existingChannels === null) {
        const team_id = await getDefaultTeamID();
        existingChannels = await Client4.getChannels(team_id);
    }
    return existingChannels;

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
        console.log('teams', teams)
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

async function updateMMChannel(name, channel) {
    if (!channel.team_id) channel.team_id = await getDefaultTeamID();
    if (!channel.display_name) channel.display_name = name;
    if (!channel.type) channel.type = 'O';
    const existingChannels = await getMMChannelList();
    const existingChannel = existingChannels.find(c => c.name === name);
    console.log('name', name)
    if (existingChannel) {
        channel.id = existingChannel.id;
        console.info("Updating channel: " + name);
        return await Client4.updateChannel(channel)
    } else {
        console.info("Inserting channel: " + name);
        return await Client4.createChannel(channel)
    }
}
