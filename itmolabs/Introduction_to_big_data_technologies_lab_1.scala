import org.apache.spark.sql.SparkSession
// import org.apache.spark.{SparkConf, SparkContext}

// LAB 1
// LOUIS SALOME

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    System.setProperty("hadoop.home.dir","C:\\winutils")

    val path = "C://Users/Louis/IdeaProjects/src/bgdata_small/"

    val spark = SparkSession.builder()
      .appName("main")
      .master("local[*]")
      .getOrCreate()

    var req : String = ""

    // import spark.implicits._


    // 1. COUNT posts, posts_comments, likes made by each user
    spark.read.parquet(path+"userWallPosts.parquet").createOrReplaceTempView("posts" )
    spark.read.parquet(path+"userWallLikes.parquet").createOrReplaceTempView("likes")
    spark.read.parquet(path+"userWallComments.parquet").createOrReplaceTempView("comments")

    req = "SELECT AB.owner_id, AB.count_posts, AB.count_likes, C.count_comments FROM (" +
                  "( SELECT A.owner_id, A.count_posts, B.count_likes FROM " +
                                    "(SELECT COUNT(*) AS count_posts, owner_id FROM posts GROUP BY owner_id) AS A" +
                             " INNER JOIN" +
                                    "(SELECT COUNT(*) AS count_likes, ownerId as owner_id FROM likes GROUP BY ownerId) AS B" +
                             " ON A.owner_id = B.owner_id" +
                                      ") AS AB " +
                       " INNER JOIN" +
                              "(SELECT COUNT(*) AS count_comments, from_id AS owner_id FROM comments GROUP BY from_id) AS C" +
                        " ON AB.owner_id=C.owner_id" +
                 ")" +
              "ORDER BY count_posts DESC"


    // 2. Count of friends, followers, users groups , photos, (gifts, videos, audios,)
    /*
    friends     : friends.parquet
    followers   : followers.parquet
    users groups: groupsProfiles.parquet
    photos      : userWallPhotos.parquet
    */
    spark.read.parquet(path+"friends.parquet").createOrReplaceTempView("friends" )
    spark.read.parquet(path+"followers.parquet").createOrReplaceTempView("followers" )
    spark.read.parquet(path+"groupsProfiles.parquet").createOrReplaceTempView("user_groups" )
    spark.read.parquet(path+"userWallPhotos.parquet").createOrReplaceTempView("photos" )

    req = "SELECT friends_followers.user_id, friends_followers.count_friends, friends_followers.count_followers," +
                      "user_groups_photos.count_photos, user_groups_photos.count_user_groups FROM" +
      "(" +
                      " (SELECT friendsCount.user_id, friendsCount.count_friends, followersCount.count_followers FROM ( " +
                                 " (SELECT COUNT(*) AS count_friends,   profile AS user_id FROM friends   GROUP BY profile) AS friendsCount" +
                      " INNER JOIN (SELECT COUNT(*) AS count_followers, profile AS user_id FROM followers GROUP BY profile) AS followersCount" +
                      " ON friendsCount.user_id = followersCount.user_id  ) " +
                        ") AS friends_followers" +
            " INNER JOIN" +
                    " (SELECT user_groupsCount.user_id, user_groupsCount.count_user_groups, photosCount.count_photos FROM ( " +
                               " (SELECT COUNT(*) AS count_user_groups,   id       AS user_id FROM user_groups   GROUP BY id) AS user_groupsCount" +
                    " INNER JOIN (SELECT COUNT(*) AS count_photos,        owner_id AS user_id FROM photos        GROUP BY owner_id) AS photosCount" +
                    " ON user_groupsCount.user_id = photosCount.user_id  ) " +
                       ") AS user_groups_photos" +
            " ON friends_followers.user_id = user_groups_photos.user_id" +
      ")"


    // 3. Count of "incoming" (made by users) comments, max and mean "incoming" comments per post
    spark.read.parquet(path+"userWallComments.parquet").createOrReplaceTempView("comments")
    spark.read.parquet(path+"userWallPosts.parquet").createOrReplaceTempView("posts" )

    req = " SELECT owner_id, " +
                        "SUM(nb_of_comments) AS count_comments, " +
                        "MAX(nb_of_comments) AS max_comments, " +
                        "MEAN(nb_of_comments) AS mean_comments  FROM " +
      " (" +
      " (SELECT post_id, COUNT(*) AS nb_of_comments FROM comments GROUP BY post_id) AS C " +
      " INNER JOIN " +
      " (SELECT id, owner_id FROM posts) AS P " +
      " ON C.post_id = P.id  " +
      " ) " +
      " GROUP BY owner_id"


    // 4. Count of "incoming" (made by users) likes, max and mean "incoming" likes per post
    spark.read.parquet(path+"userWallLikes.parquet").createOrReplaceTempView("likes")
    spark.read.parquet(path+"userWallPosts.parquet").createOrReplaceTempView("posts" )

    req = " SELECT owner_id, " +
      "SUM(nb_of_likes) AS count_likes, " +
      "MAX(nb_of_likes) AS max_likes, " +
      "MEAN(nb_of_likes) AS mean_likes FROM " +
      " (" +
      " (SELECT itemId as post_Id, COUNT(*) AS nb_of_likes FROM likes GROUP BY itemId) AS L " +
      " INNER JOIN " +
      " (SELECT id, owner_id FROM posts) AS P " +
      " ON L.post_id = P.id  " +
      " ) " +
      " GROUP BY owner_id"


    // 5. Count open/closed (e.g. private) groups a user participates in
    // groups.is_closed \in {0,1}
    spark.read.parquet(path+"groupsProfiles.parquet").createOrReplaceTempView("groups")
    spark.read.parquet(path+"userGroupsSubs.parquet").createOrReplaceTempView("subs")

    req = "SELECT subs.user as user_id, " +
              "count(is_closed) AS count_closed, " +
              "count(is_open) AS count_opened " +
                  " FROM subs " +
                      " LEFT JOIN  " +
                          "(SELECT groups.key as group_id, 1 as is_open   FROM groups WHERE groups.is_closed == 0) AS O " +
                          " ON subs.group == O.group_id " +
                      " LEFT JOIN " +
                          "(SELECT groups.key as group_id, 1 as is_closed FROM groups WHERE groups.is_closed == 1) AS C" +
                          " ON subs.group == C.group_id" +
              " GROUP BY subs.user"


    // 6. a. count of deleted users in followers
    spark.read.parquet(path+"followerProfiles.parquet").createOrReplaceTempView("followers") // key (or id) & deactivated in {null, banned}

    req = "SELECT COUNT(*) as deactivated FROM (SELECT * FROM  WHERE deactivated = 'banned' "


    // 6. b. count of deleted users in friends
    spark.read.parquet(path+"friendsProfiles.parquet").createOrReplaceTempView("friends") // key (or id) & deactivated in {null, banned}

    req = "SELECT COUNT(*) as deactivated_friends FROM friends WHERE deactivated = 'banned'"


    // 7. a. aggregate for comments and likes separately) made by friends
    spark.read.parquet(path+"userWallLikes.parquet").createOrReplaceTempView("likes") // likerId, itemId
    spark.read.parquet(path+"friends.parquet").createOrReplaceTempView("friends") // profile, follower
    spark.read.parquet(path+"userWallPosts.parquet").createOrReplaceTempView("posts" ) // id, owner_id

    req = "SELECT owner_id AS user_id, COUNT(*) AS number_of_likes_from_friends FROM" +
      " (" +
        "( (SELECT likerId, itemID FROM likes) AS L" +
        "  INNER JOIN" +
        "  (SELECT owner_id, id FROM posts) AS P" +
        " ON L.itemID = P.id" +
      "  ) AS LP" +
      "" +
      " INNER JOIN" +
      "" +
      "  (SELECT profile, follower FROM friends) AS F" +
      " ON F.follower = LP.likerId" +
      " )" +
      " WHERE profile=owner_id" +
      " GROUP BY owner_id"


    // 7. b. aggregate (e.g. cont, max, mean) characteristics for comments and likes separately) made by followers
    spark.read.parquet(path+"userWallLikes.parquet").createOrReplaceTempView("likes") // likerId, itemId
    spark.read.parquet(path+"friends.parquet").createOrReplaceTempView("friends") // profile, follower
    spark.read.parquet(path+"userWallPosts.parquet").createOrReplaceTempView("posts" ) // id, owner_id

    req = "SELECT owner_id AS user_id, COUNT(*) AS number_of_likes_from_friends FROM" +
      " (" +
      "( (SELECT likerId, itemID FROM likes) AS L" +
      "  INNER JOIN" +
      "  (SELECT owner_id, id FROM posts) AS P" +
      " ON L.itemID = P.id" +
      "  ) AS LP" +
      "" +
      " INNER JOIN" +
      "" +
      "  (SELECT profile, follower FROM friends) AS F" +
      " ON F.follower = LP.likerId" +
      " )" +
      " WHERE profile=owner_id" +
      " GROUP BY owner_id"


    // 8. a. find emoji in user's posts, and count negative, positive, others

    // 8. b. find emoji in user's user's comments, and count negative, positive, others

    // 9. join all users data from each tasks to the one table


    spark.sql(req)
      .show()
  }
}
