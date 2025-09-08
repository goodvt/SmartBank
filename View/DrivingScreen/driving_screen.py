from View.base_screen import BaseScreenView
from kivy.properties import ObjectProperty, NumericProperty, ListProperty, StringProperty

class DrivingScreenView(BaseScreenView):


    #déclaration des propriété dans la vue
    date_imputation = StringProperty('')

    def model_is_changed(self) -> None:
        """
        Called whenever any change has occurred in the data model.
        The view in this method tracks these changes and updates the UI
        according to these changes.
        """
