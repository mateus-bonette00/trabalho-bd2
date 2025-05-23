from django.db import models
from django.contrib.auth.models import User

class Game(models.Model):
    """Jogos da Twitch"""
    twitch_id = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=255)
    box_art_url = models.URLField(blank=True, null=True)
    igdb_id = models.CharField(max_length=50, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'games'
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['twitch_id']),
        ]
    
    def __str__(self):
        return self.name

class StreamerUser(models.Model):
    """Streamers da Twitch"""
    twitch_id = models.CharField(max_length=50, unique=True)
    login = models.CharField(max_length=25, unique=True)
    display_name = models.CharField(max_length=255)
    type = models.CharField(max_length=50, default='')
    broadcaster_type = models.CharField(max_length=50, default='')
    description = models.TextField(blank=True)
    profile_image_url = models.URLField(blank=True)
    offline_image_url = models.URLField(blank=True)
    view_count = models.BigIntegerField(default=0)
    created_at = models.DateTimeField()
    
    class Meta:
        db_table = 'streamers'
        indexes = [
            models.Index(fields=['login']),
            models.Index(fields=['display_name']),
            models.Index(fields=['view_count']),
        ]
    
    def __str__(self):
        return self.display_name

class Stream(models.Model):
    """Streams ativas"""
    twitch_id = models.CharField(max_length=50, unique=True)
    user = models.ForeignKey(StreamerUser, on_delete=models.CASCADE, related_name='streams')
    game = models.ForeignKey(Game, on_delete=models.SET_NULL, null=True, related_name='streams')
    title = models.TextField()
    viewer_count = models.IntegerField()
    started_at = models.DateTimeField()
    language = models.CharField(max_length=10)
    thumbnail_url = models.URLField()
    tag_ids = models.JSONField(default=list, blank=True)
    is_mature = models.BooleanField(default=False)
    collected_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'streams'
        indexes = [
            models.Index(fields=['viewer_count']),
            models.Index(fields=['started_at']),
            models.Index(fields=['language']),
            models.Index(fields=['collected_at']),
        ]
    
    def __str__(self):
        return f"{self.user.display_name} - {self.title[:50]}"

class Clip(models.Model):
    """Clips populares"""
    twitch_id = models.CharField(max_length=50, unique=True)
    url = models.URLField()
    embed_url = models.URLField()
    broadcaster = models.ForeignKey(StreamerUser, on_delete=models.CASCADE, related_name='clips')
    game = models.ForeignKey(Game, on_delete=models.SET_NULL, null=True, related_name='clips')
    title = models.CharField(max_length=255)
    view_count = models.IntegerField()
    created_at = models.DateTimeField()
    thumbnail_url = models.URLField()
    duration = models.FloatField()  # em segundos
    vod_offset = models.IntegerField(null=True, blank=True)
    
    class Meta:
        db_table = 'clips'
        indexes = [
            models.Index(fields=['view_count']),
            models.Index(fields=['created_at']),
            models.Index(fields=['duration']),
        ]
    
    def __str__(self):
        return f"{self.title} - {self.broadcaster.display_name}"