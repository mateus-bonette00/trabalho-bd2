from django.core.management.base import BaseCommand
from twitch_etl.etl_service import TwitchETLService

class Command(BaseCommand):
    help = 'Execute o processo ETL da API da Twitch'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--games-only',
            action='store_true',
            help='Extrair apenas jogos',
        )
        parser.add_argument(
            '--streams-only',
            action='store_true',
            help='Extrair apenas streams',
        )
        parser.add_argument(
            '--clips-only',
            action='store_true',
            help='Extrair apenas clips',
        )
    
    def handle(self, *args, **options):
        etl_service = TwitchETLService()
        
        if options['games_only']:
            etl_service.extract_games()
        elif options['streams_only']:
            etl_service.extract_streams()
        elif options['clips_only']:
            etl_service.extract_clips()
        else:
            etl_service.run_full_etl()
        
        self.stdout.write(
            self.style.SUCCESS('ETL executado com sucesso!')
        )