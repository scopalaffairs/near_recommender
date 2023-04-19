Recommendation system logic:
If the user is new (< 1 week, < 3 days):
→ trending users
If the user is not active:
→ trending users
If the user is active:
→ friends-of-friends
If the user has a tag:
→ tag similarity
If the user has posted
→ post similarity
If the user was inactive for some period:
→ trending users
Bot detection (identifies known bots like littlelion, mr27, hypefairy):
high % active days
high post per day
low follow_ratio
…
but difficult to filter out other account without further analysis of content, etc
Definition of active users (proposals):
following more than 5 users
has any network activity (posted/liked/poked/added a tag)
additional timeframe
