from django.db import connection
from django.db.models import Q, Count, Sum, Avg, Max, Min
from .models import Game, StreamerUser, Stream, Clip

class DynamicQueryBuilder:
    """Construtor de consultas dinâmicas para relatórios ad hoc"""
    
    AVAILABLE_TABLES = {
        'games': Game,
        'streamers': StreamerUser,
        'streams': Stream,
        'clips': Clip
    }
    
    AGGREGATION_FUNCTIONS = {
        'count': Count,
        'sum': Sum,
        'avg': Avg,
        'max': Max,
        'min': Min
    }
    
    def __init__(self):
        self.selected_tables = []
        self.selected_fields = []
        self.filters = []
        self.aggregations = []
        self.order_by = []
    
    def add_table(self, table_name):
        """Adicionar tabela à consulta"""
        if table_name in self.AVAILABLE_TABLES:
            self.selected_tables.append(table_name)
    
    def add_field(self, table_name, field_name, alias=None):
        """Adicionar campo à seleção"""
        field_key = f"{table_name}__{field_name}" if '__' not in field_name else field_name
        self.selected_fields.append({
            'field': field_key,
            'alias': alias or f"{table_name}_{field_name}"
        })
    
    def add_filter(self, field, operator, value, logical_op='AND'):
        """Adicionar filtro à consulta"""
        self.filters.append({
            'field': field,
            'operator': operator,
            'value': value,
            'logical_op': logical_op
        })
    
    def add_aggregation(self, field, function, alias=None):
        """Adicionar agregação"""
        self.aggregations.append({
            'field': field,
            'function': function,
            'alias': alias or f"{function}_{field}"
        })
    
    def build_query(self):
        """Construir e executar a consulta"""
        if not self.selected_tables:
            raise ValueError("Nenhuma tabela selecionada")
        
        # Começar com a primeira tabela
        main_table = self.selected_tables[0]
        queryset = self.AVAILABLE_TABLES[main_table].objects.all()
        
        # Aplicar joins se necessário
        if len(self.selected_tables) > 1:
            queryset = self._apply_joins(queryset)
        
        # Aplicar filtros
        if self.filters:
            queryset = self._apply_filters(queryset)
        
        # Aplicar seleção de campos
        if self.selected_fields:
            queryset = self._apply_field_selection(queryset)
        
        # Aplicar agregações
        if self.aggregations:
            queryset = self._apply_aggregations(queryset)
        
        return queryset
    
    def _apply_joins(self, queryset):
        """Aplicar joins entre tabelas"""
        # Lógica simplificada - expandir conforme necessário
        select_related = []
        prefetch_related = []
        
        for table in self.selected_tables[1:]:
            if table == 'streamers' and 'streams' in self.selected_tables:
                select_related.append('user')
            elif table == 'games' and ('streams' in self.selected_tables or 'clips' in self.selected_tables):
                select_related.append('game')
        
        if select_related:
            queryset = queryset.select_related(*select_related)
        if prefetch_related:
            queryset = queryset.prefetch_related(*prefetch_related)
        
        return queryset
    
    def _apply_filters(self, queryset):
        """Aplicar filtros à consulta"""
        q_objects = Q()
        
        for i, filter_item in enumerate(self.filters):
            field = filter_item['field']
            operator = filter_item['operator']
            value = filter_item['value']
            logical_op = filter_item['logical_op']
            
            # Construir filtro baseado no operador
            if operator == 'equals':
                condition = Q(**{field: value})
            elif operator == 'contains':
                condition = Q(**{f"{field}__icontains": value})
            elif operator == 'gt':
                condition = Q(**{f"{field}__gt": value})
            elif operator == 'lt':
                condition = Q(**{f"{field}__lt": value})
            elif operator == 'gte':
                condition = Q(**{f"{field}__gte": value})
            elif operator == 'lte':
                condition = Q(**{f"{field}__lte": value})
            else:
                continue
            
            # Aplicar operador lógico
            if i == 0:
                q_objects = condition
            elif logical_op == 'AND':
                q_objects &= condition
            elif logical_op == 'OR':
                q_objects |= condition
        
        return queryset.filter(q_objects)
    
    def _apply_field_selection(self, queryset):
        """Aplicar seleção de campos específicos"""
        if self.aggregations:
            # Se há agregações, usar values() com os campos de agrupamento
            group_fields = [f['field'] for f in self.selected_fields]
            return queryset.values(*group_fields)
        else:
            # Seleção normal de campos
            field_names = [f['field'] for f in self.selected_fields]
            return queryset.values(*field_names)
    
    def _apply_aggregations(self, queryset):
        """Aplicar funções de agregação"""
        if not self.aggregations:
            return queryset
        
        annotation_dict = {}
        for agg in self.aggregations:
            field = agg['field']
            function = agg['function']
            alias = agg['alias']
            
            if function in self.AGGREGATION_FUNCTIONS:
                annotation_dict[alias] = self.AGGREGATION_FUNCTIONS[function](field)
        
        return queryset.annotate(**annotation_dict)