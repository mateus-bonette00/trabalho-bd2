import requests
import json
from datetime import datetime
from django.conf import settings
from .models import Game, StreamerUser, Stream, Clip

class TwitchETLService:
    """Serviço para extrair dados da API da Twitch"""
    
    def __init__(self):
        self.client_id = settings.TWITCH_CLIENT_ID
        self.client_secret = settings.TWITCH_CLIENT_SECRET
        self.access_token = None
        self.base_url = "https://api.twitch.tv/helix"
    
    def get_access_token(self):
        """Obter token de acesso da Twitch"""
        url = "https://id.twitch.tv/oauth2/token"
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        
        response = requests.post(url, params=params)
        if response.status_code == 200:
            self.access_token = response.json()['access_token']
            return True
        return False
    
    def make_request(self, endpoint, params=None):
        """Fazer requisição para a API da Twitch"""
        if not self.access_token:
            self.get_access_token()
        
        headers = {
            'Client-ID': self.client_id,
            'Authorization': f'Bearer {self.access_token}'
        }
        
        response = requests.get(f"{self.base_url}/{endpoint}", 
                              headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Erro na requisição: {response.status_code}")
            return None
    
    def extract_games(self, limit=100):
        """Extrair jogos populares"""
        games_data = self.make_request("games/top", {"first": limit})
        
        if games_data and 'data' in games_data:
            for game_info in games_data['data']:
                Game.objects.update_or_create(
                    twitch_id=game_info['id'],
                    defaults={
                        'name': game_info['name'],
                        'box_art_url': game_info.get('box_art_url', ''),
                        'igdb_id': game_info.get('igdb_id', '')
                    }
                )
            print(f"Processados {len(games_data['data'])} jogos")
    
    def extract_streams(self, limit=100):
        """Extrair streams ativas"""
        streams_data = self.make_request("streams", {"first": limit})
        
        if streams_data and 'data' in streams_data:
            # Primeiro, extrair informações dos usuários únicos
            user_ids = list(set([stream['user_id'] for stream in streams_data['data']]))
            self.extract_users_by_ids(user_ids)
            
            for stream_info in streams_data['data']:
                try:
                    user = StreamerUser.objects.get(twitch_id=stream_info['user_id'])
                    game = None
                    
                    if stream_info['game_id']:
                        game, _ = Game.objects.get_or_create(
                            twitch_id=stream_info['game_id'],
                            defaults={'name': stream_info.get('game_name', 'Unknown')}
                        )
                    
                    Stream.objects.update_or_create(
                        twitch_id=stream_info['id'],
                        defaults={
                            'user': user,
                            'game': game,
                            'title': stream_info['title'],
                            'viewer_count': stream_info['viewer_count'],
                            'started_at': datetime.fromisoformat(
                                stream_info['started_at'].replace('Z', '+00:00')
                            ),
                            'language': stream_info['language'],
                            'thumbnail_url': stream_info['thumbnail_url'],
                            'tag_ids': stream_info.get('tag_ids', []),
                            'is_mature': stream_info.get('is_mature', False)
                        }
                    )
                except StreamerUser.DoesNotExist:
                    print(f"Usuário {stream_info['user_id']} não encontrado")
                    continue
            
            print(f"Processadas {len(streams_data['data'])} streams")
    
    def extract_users_by_ids(self, user_ids):
        """Extrair informações de usuários por IDs"""
        # API permite até 100 IDs por request
        for i in range(0, len(user_ids), 100):
            batch_ids = user_ids[i:i+100]
            params = {'id': batch_ids}
            
            users_data = self.make_request("users", params)
            
            if users_data and 'data' in users_data:
                for user_info in users_data['data']:
                    StreamerUser.objects.update_or_create(
                        twitch_id=user_info['id'],
                        defaults={
                            'login': user_info['login'],
                            'display_name': user_info['display_name'],
                            'type': user_info.get('type', ''),
                            'broadcaster_type': user_info.get('broadcaster_type', ''),
                            'description': user_info.get('description', ''),
                            'profile_image_url': user_info.get('profile_image_url', ''),
                            'offline_image_url': user_info.get('offline_image_url', ''),
                            'view_count': user_info.get('view_count', 0),
                            'created_at': datetime.fromisoformat(
                                user_info['created_at'].replace('Z', '+00:00')
                            )
                        }
                    )
    
    def extract_clips(self, limit=100, started_at=None, ended_at=None):
        """Extrair clips populares"""
        params = {"first": limit}
        if started_at:
            params['started_at'] = started_at
        if ended_at:
            params['ended_at'] = ended_at
            
        clips_data = self.make_request("clips", params)
        
        if clips_data and 'data' in clips_data:
            # Extrair IDs únicos de usuários e jogos
            user_ids = list(set([clip['broadcaster_id'] for clip in clips_data['data']]))
            game_ids = list(set([clip['game_id'] for clip in clips_data['data'] if clip['game_id']]))
            
            self.extract_users_by_ids(user_ids)
            
            for clip_info in clips_data['data']:
                try:
                    broadcaster = StreamerUser.objects.get(twitch_id=clip_info['broadcaster_id'])
                    game = None
                    
                    if clip_info['game_id']:
                        game, _ = Game.objects.get_or_create(
                            twitch_id=clip_info['game_id'],
                            defaults={'name': 'Unknown Game'}
                        )
                    
                    Clip.objects.update_or_create(
                        twitch_id=clip_info['id'],
                        defaults={
                            'url': clip_info['url'],
                            'embed_url': clip_info['embed_url'],
                            'broadcaster': broadcaster,
                            'game': game,
                            'title': clip_info['title'],
                            'view_count': clip_info['view_count'],
                            'created_at': datetime.fromisoformat(
                                clip_info['created_at'].replace('Z', '+00:00')
                            ),
                            'thumbnail_url': clip_info['thumbnail_url'],
                            'duration': clip_info['duration'],
                            'vod_offset': clip_info.get('vod_offset')
                        }
                    )
                except StreamerUser.DoesNotExist:
                    print(f"Broadcaster {clip_info['broadcaster_id']} não encontrado")
                    continue
            
            print(f"Processados {len(clips_data['data'])} clips")
    
    def run_full_etl(self):
        """Executar ETL completo"""
        print("Iniciando ETL da Twitch...")
        
        print("1. Extraindo jogos...")
        self.extract_games(100)
        
        print("2. Extraindo streams...")
        self.extract_streams(100)
        
        print("3. Extraindo clips...")
        self.extract_clips(100)
        
        print("ETL concluído!")