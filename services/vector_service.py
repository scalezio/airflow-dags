import math
from operator import itemgetter

from services.attributes_mapping_service import AttributesMappingService

from utils.singelton import SingletonDecorator


@SingletonDecorator
class VectorService:
    def __init__(self):
        mapping_service = AttributesMappingService()
        self.mappings = mapping_service.provide_attributes_mappings()
        self.attributes_mapping = {k: v for mapping in self.mappings.values() for k, v in mapping.items()}
        self.flows_mapping = {k: i for i, k in enumerate(sorted(self.mappings.keys()))}

    def calculate_attributes_vector(self, flow: str, attributes: dict):
        """
        calculate one vector for all the attributes from all the flows
        one value per one attributes is allowed
        """
        mapping = self.attributes_mapping

        return [self.flows_mapping.get(flow)] + [
            mapping.get(att).get(attributes.get(att), -1.0) if att in attributes else -1.0
            for att in sorted(mapping.keys())]

    def calculate_flow_attributes_vector(self, flow: str, attributes: dict):
        """
        calculate one vector for all the attributes from specific flow
        one value per one attributes is allowed
        """
        mapping = self.mappings.get(flow)

        return [mapping.get(att).get(attributes.get(att), -1.0) if att in attributes else -1.0
                for att in sorted(mapping.keys())]

    def get_vector_labels(self, flows, only_product_attributes=False):
        mapping = self.attributes_mapping
        return [att for att in mapping.keys() if att.split('_') and
                att.split('_')[0] in flows] if only_product_attributes else mapping.keys()

    def calculate_mean_flow_attributes_vector(self, attributes: dict, flows=None, only_product_attributes=False):
        """
        calculate mean vector for all the attributes from specific flow
        mean of values from same attribute
        """
        mapping = self.attributes_mapping

        att_mapping = {att: [(mapping.get(att, {}).get(att_name) * count, count) for val in attribute
                             for att_name, count in val.items() if mapping.get(att, {}).get(att_name)]
                       for att, attribute in attributes.items()}
        att_mapping_mean = {att: sum(map(itemgetter(0), att_values)) / sum(map(itemgetter(1), att_values)) for
                            att, att_values in att_mapping.items() if att_values}

        attributes_values = [att for att in mapping.keys() if att.split('_') and
                             att.split('_')[0] in flows] if only_product_attributes else mapping.keys()

        return [att_mapping_mean.get(att, -1.0) if att in attributes else -1.0 for att in sorted(attributes_values)]

    def calculate_hierarchy_vector(self, flow: str, hierarchy: dict):
        mapping = self.mappings.get(flow)
        return [math.pow(10, -hierarchy.get(att)) if hierarchy.get(att) is not None else 0.0 for att in
                sorted(mapping.keys())]

    @staticmethod
    def calculate_similarity(u, v, h):
        numerator = sum([
            (((math.copysign(1, u[i]) + 1) * (math.copysign(1, v[i]) + 1)) / 4) * math.pow(u[i] - v[i], 2) * h[i] for i
            in range(len(u))])

        denominator = sum(
            [(((math.copysign(1, u[i]) + 1) * (math.copysign(1, v[i]) + 1)) / 4) * h[i] for i in range(len(u))])

        similarity = math.pow(2, - (math.sqrt(numerator) / denominator)) if denominator > 0 else 0
        return similarity

    def calculate_distance(self, u, v, h):
        return 1 - self.calculate_similarity(u, v, h)
