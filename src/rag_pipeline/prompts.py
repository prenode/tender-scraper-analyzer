from enum import Enum


class Prompts(Enum):
    """
    Enum class `Prompts` contains predefined prompt strings for various sections of a tender document.
    """
    BEKANNTMACHUNG_SUMMARY = "Bitte fasse zusammen: 1. Wie Angebote eingereicht werden dürfen 2. Verfahrensart 3. Die Art und den Umfang der Leistung 4. Die Ausführungsfrist/die Länge des Auftrags 5. Sonstiges"
    DOCUMENTS_DESCRIPTION = "Was ist der Gegenstand der Beschaffung der Ausschreibung? Beschreibe kurz, welche Leistungen im Rahmen der Ausschreibung interessant sind?"
    CERTIFICATIONS = "Wird in der Ausschreibung eine bestimmte Zertifizierung oder Qualifikation gefordert? Falls ja, welche?"
    REQUIREMENTS_OFFER = (
        "Welche Anforderungen müssen Angebote erfüllen, um berücksichtigt zu werden?"
    )
