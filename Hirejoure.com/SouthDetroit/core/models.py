from django.db import models
from django.conf import settings


class User(models.Model):
    """Core user profile. We keep the row narrow; optional fields separated in Profile."""

    name = models.CharField(max_length=100)
    email = models.EmailField(unique=True)
    password_hash = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    last_login = models.DateTimeField(blank=True, null=True)

    bio = models.TextField(blank=True)
    location = models.CharField(max_length=120, blank=True)
    avatar_url = models.TextField(blank=True)

    def __str__(self):
        return f"{self.name} <{self.email}>"

    class Meta:
        ordering = ("-created_at",)


class Friendship(models.Model):
    PENDING = "pending"
    ACCEPTED = "accepted"
    BLOCKED = "blocked"

    STATUS_CHOICES = [
        (PENDING, "Pending"),
        (ACCEPTED, "Accepted"),
        (BLOCKED, "Blocked"),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="friendships_from")
    friend = models.ForeignKey(User, on_delete=models.CASCADE, related_name="friendships_to")
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=PENDING)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("user", "friend")
        indexes = [
            models.Index(fields=["user", "status"]),
        ]

    def __str__(self):
        return f"{self.user} ↔ {self.friend} ({self.status})"


class Post(models.Model):
    PUBLIC = "public"
    FRIENDS = "friends"
    PRIVATE = "private"

    PRIVACY_CHOICES = [
        (PUBLIC, "Public"),
        (FRIENDS, "Friends"),
        (PRIVATE, "Private"),
    ]

    author = models.ForeignKey(User, on_delete=models.CASCADE, related_name="posts")
    body = models.TextField(blank=True)
    media_url = models.TextField(blank=True)
    privacy = models.CharField(max_length=10, choices=PRIVACY_CHOICES, default=FRIENDS)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["author", "-created_at"]),
        ]

    def __str__(self):
        return f"Post {self.id} by {self.author}"


class Reaction(models.Model):
    LIKE = "like"
    LOVE = "love"
    WOW = "wow"

    REACTION_CHOICES = [
        (LIKE, "Like"),
        (LOVE, "Love"),
        (WOW, "Wow"),
    ]

    post = models.ForeignKey(Post, on_delete=models.CASCADE, related_name="reactions")
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="reactions")
    reaction = models.CharField(max_length=12, choices=REACTION_CHOICES, default=LIKE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("post", "user")
        indexes = [models.Index(fields=["post"])]

    def __str__(self):
        return f"{self.user} {self.reaction} on {self.post_id}"


class Comment(models.Model):
    post = models.ForeignKey(Post, on_delete=models.CASCADE, related_name="comments")
    author = models.ForeignKey(User, on_delete=models.CASCADE, related_name="comments")
    body = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("created_at",)
        indexes = [models.Index(fields=["post", "created_at"])]

    def __str__(self):
        return f"Comment {self.id} on {self.post_id} by {self.author_id}"


class Photo(models.Model):
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name="photos")
    cdn_key = models.TextField(unique=True)
    width = models.IntegerField(null=True, blank=True)
    height = models.IntegerField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.cdn_key

    class Meta:
        ordering = ("-created_at",)
