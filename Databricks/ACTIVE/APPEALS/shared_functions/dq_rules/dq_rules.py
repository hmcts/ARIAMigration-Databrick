from abc import ABC, abstractmethod


class DQRulesBase(ABC):

    @abstractmethod
    def add_checks(checks={}):
        pass
