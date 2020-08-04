var fixShardNames = function(dbName="config", dryRun=true, verbose=false) {

    function printIfVerbose(arg) {
        if (verbose == true)
            printjson(arg);
    };

    function detectDryRun() {
      if (dryRun == true) print("The script is running in dry run mode. No modifications will be done to the metadata");
    };

    function runPreFlightChecks() {
      print("Running pre-flight checks");
      checkIfDbExists(dbName);
      checkIfMongos();
      checkIfLiveCSRS();
      checkServerVersion();
      print("Pre-flight checks completed successfully");
    };

    function checkIfDbExists(dbName) {
      const res = db.adminCommand({"listDatabases": 1});
      assert(res, "The listDatabases command failed");
      assert(res.hasOwnProperty("ok"), "The ok field is not present");
      assert.eq(1, res.ok, "Failed to obtain the list of databases");
      assert(res.hasOwnProperty("databases"), "The databases field is not present");
      var dbFound = false;
      res.databases.forEach(function(dbDoc) {
        assert(dbDoc.hasOwnProperty("name"), "The name field is no present");
        if(dbDoc.name === dbName) {
          dbFound = true;
          assert(dbDoc.hasOwnProperty("empty"), "The empty field is not present");
          assert.eq(dbDoc.empty, false, "The target database is empty");
        };
      });
      assert.eq(dbFound, true, "The target database could not be located");
    };

    // To make sure we are not running the script on a live CSRS
    function checkIfLiveCSRS() {
      const res = db.serverCmdLineOpts();
      assert(res, "The serverCmdLineOpts command failed");
      assert(res.hasOwnProperty("ok"), "The ok field is not present");
      assert.eq(1, res.ok, "Failed to obtain the startup configuration parameters");
      assert(res.hasOwnProperty("parsed"), "The parsed field is not present");
      assert(!res.parsed.hasOwnProperty("sharding"), "This server is running with sharding enabled. The CSRS should be running in standalone or non-configsvr replica set mode for the script to work");
    };

    // To make sure we are not running the script on a mongos
    function checkIfMongos() {
      const res = db.serverStatus();
      assert(res, "The serverStatus command failed");
      assert(res.hasOwnProperty("ok"), "The ok field is not present");
      assert.eq(1, res.ok, "Failed to run serverStatus command");
      assert(res.hasOwnProperty("process"), "The process field is not present");
      assert.neq("mongos", res.process, "Mongos detected but this script is not meant to be run on a mongos");
    };

    function checkServerVersion() {
      const supportedVersions = [ "36", "40", "42" ];
      const res = db.serverStatus();
      assert(res, "The serverStatus command failed");
      assert(res.hasOwnProperty("ok"), "The ok field is not present");
      assert.eq(1, res.ok, "Failed to run serverStatus command");
      assert(res.hasOwnProperty("version"), "The version field is not present");
      const verArray = res.version.split(".");
      const majVer = verArray[0] + verArray[1];
      assert.eq(true, supportedVersions.includes(majVer), "Unsupported version detected. Please reach out to Technical Support for assistance");
    };

    function countShardIdDocs(shardId) {
      const chunkCount = db.chunks.count({"shard": shardId});
      const dbCount = db.databases.count({"primary": shardId});
      const shardsCount = db.shards.count({"_id": shardId});
      return chunkCount + dbCount + shardsCount;
    };

    function removeShard(oldId) {
      // we can't update "_id" and there is a unique index on "host" thus we need to remove the shard document first
      const ret = db.shards.remove({"_id": oldId});
      assert(ret, "Shard document could not be removed: " + oldId);
      assert(ret.hasOwnProperty("nRemoved"), "The nRemoved field is not present");
      assert.eq(1, ret.nRemoved, "Failed to remove shard document: \n shardId: " + oldId + " ret: " + ret);
      printIfVerbose(ret);
    };

    function insertShard(newDoc) {
      const ret = db.shards.insert(newDoc);
      assert(ret, "Shard document could not be inserted: \n newDoc: " + newDoc + " ret: " + ret);
      assert(ret.hasOwnProperty("nInserted"), "The nInserted field is not present");
      assert.eq(1, ret.nInserted, "Failed to insert shard document: \n newDoc: " + newDoc + " ret: " + ret);
      printIfVerbose(ret);
    };

    function fixShards(oldId, newDoc) {
      print("Updating config.shards...");
      assert.neq(oldId, newDoc._id, "New shard Id should be different from the old shard Id");
      removeShard(oldId);
      insertShard(newDoc);
      print("Done updating config.shards");
    };
    
    function fixDatabases(oldId, newId) {
      print("Updating config.databases...");
      const ret = db.databases.update({"primary": oldId}, {"$set": {"primary": newId}}, {multi: true, writeConcern: { w: "majority", wtimeout: 5000 }});
      assert(ret, "Shard document could not be updated:  \n shardId: " + oldId + " ret: " + ret);
      assert(ret.hasOwnProperty("nMatched"), "The nRemoved field is not present in the resulting document");
      assert(ret.hasOwnProperty("nUpserted"), "The nUpserted field is not present in the resulting document");
      assert(ret.hasOwnProperty("nModified"), "The nModified field is not present in the resulting document");
      // It is okay for a shard to not have any chunks
      // assert.gt(ret.nMatched, 0, "No matching chunk documents found \n shardId: " + oldId + " ret: " + ret);
      assert.eq(0, ret.nUpserted, "Updating shard documents resulted into an upsert: \n shardId: " + oldId + " ret: " + ret);
      assert.eq(ret.nMatched, ret.nModified, "Not all of the matched database documents got updated: \n shardId: " + oldId + " ret: " + ret);;
      printIfVerbose(ret);
      print("Done updating config.databases");
    };
    
    function fixChunks(oldId, newId) {
      print("Updating config.chunks...");
      // this could potentially update a great number of documents
      const ret = db.chunks.update({"shard": oldId}, {"$set": {"shard": newId}}, {multi: true, writeConcern: { w: "majority", wtimeout: 5000 }});
      assert(ret, "Chunk documents could not be updated:  \n shardId: " + oldId + "ret: " + ret);
      assert(ret.hasOwnProperty("nMatched"), "The nRemoved field is not present in the resulting document");
      assert(ret.hasOwnProperty("nUpserted"), "The nUpserted field is not present in the resulting document");
      assert(ret.hasOwnProperty("nModified"), "The nModified field is not present in the resulting document");
      assert.gt(ret.nMatched, 0, "No matching chunk documents found \n shardId: " + oldId + "ret: " + ret);
      assert.eq(0, ret.nUpserted, "Updating chunk documents resulted into an upsert: \n shardId: " + oldId + "ret: " + ret);
      assert.eq(ret.nMatched, ret.nModified, "Not all of the matched chunk documents got updated: \n shardId: " + oldId + "ret: " + ret);
      printIfVerbose(ret);
      print("Done updating config.chunks");
    };
    
    function fix(shardDoc) {
        printIfVerbose(shardDoc);
        var ret = { _id: shardDoc._id, needsFixing: false, fixed: false };
        const oldShardId = shardDoc._id;

        // set the new shard Id to match the replica set name
        shardDoc._id = shardDoc.host.split("/")[0];
        const newShardId = shardDoc._id;
        assert.gt(newShardId.length, 0, "New shard Id is too short");

        // We only need to modify the metadata if the shard Id does not match repl set Id
        if (newShardId != oldShardId) {
            ret.needsFixing = true;
            print("Shard " + oldShardId + " does not match its replica set name " + newShardId);
            const docsToFix = countShardIdDocs(oldShardId);
            print("This metadata has " + docsToFix + " documents with old " + oldShardId + " shard id that need to be updated");
            if (dryRun === false) {
              fixShards(oldShardId, shardDoc);
              fixDatabases(oldShardId, newShardId);
              fixChunks(oldShardId, newShardId);
              const docsNotFixed = countShardIdDocs(oldShardId); // this is expected to be zero
              assert.eq(0, docsNotFixed, "Could not do all of the modifications");
              ret.fixed = true;
            };
        };

        return ret;
    };

    try {
      runPreFlightChecks();
      detectDryRun();
      
      // This is the only global that we should need
      db = db.getSiblingDB(dbName);

      const shards = db.shards.find().toArray();
      assert.gt(shards.length, 0, "No shard documents found");

      var executionResults = [];
      var res = null;
      shards.forEach(function(s) {
        res = fix(s);
        executionResults.push(res);
      });
      printjson({"Execution results": executionResults});
      print("\nScript execution is now complete");        
    } catch (err) {
      print("Something went wrong: " + err);
    };
};