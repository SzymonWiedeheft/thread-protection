"""Domain validation for ingestion service."""

from typing import List, Tuple
import structlog
from schemas import is_valid_domain, clean_domain, DomainModel
from common import ValidationError

logger = structlog.get_logger()


class DomainValidator:
    """Validator for domain data."""

    def __init__(self, strict: bool = True):
        """
        Initialize domain validator.

        Args:
            strict: If True, raise errors on validation failure;
                   If False, log warnings and filter out invalid domains
        """
        self.strict = strict
        self.stats = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "duplicates": 0,
        }

    def validate(self, domains: List[DomainModel]) -> List[DomainModel]:
        """
        Validate list of domain models.

        Args:
            domains: List of DomainModel objects to validate

        Returns:
            List of valid DomainModel objects

        Raises:
            ValidationError: If strict=True and validation fails
        """
        if not domains:
            logger.warning("No domains to validate")
            return []

        logger.info("Starting domain validation", total_count=len(domains))

        valid_domains = []
        seen_domains = set()
        invalid_count = 0
        duplicate_count = 0

        for domain_model in domains:
            self.stats["total"] += 1

            # Check if domain format is valid
            if not is_valid_domain(domain_model.domain):
                invalid_count += 1
                self.stats["invalid"] += 1

                logger.warning(
                    "Invalid domain format",
                    domain=domain_model.domain,
                    source=domain_model.source,
                )

                if self.strict:
                    raise ValidationError(
                        f"Invalid domain format: {domain_model.domain}"
                    )
                continue

            # Check for duplicates
            if domain_model.domain in seen_domains:
                duplicate_count += 1
                self.stats["duplicates"] += 1

                logger.debug(
                    "Duplicate domain found",
                    domain=domain_model.domain,
                    source=domain_model.source,
                )
                continue

            # Add to valid set
            seen_domains.add(domain_model.domain)
            valid_domains.append(domain_model)
            self.stats["valid"] += 1

        logger.info(
            "Domain validation complete",
            total=len(domains),
            valid=len(valid_domains),
            invalid=invalid_count,
            duplicates=duplicate_count,
        )

        return valid_domains

    def validate_and_clean(self, domains: List[DomainModel]) -> List[DomainModel]:
        """
        Validate and clean domain models.

        This method attempts to clean domains before validation.

        Args:
            domains: List of DomainModel objects

        Returns:
            List of valid, cleaned DomainModel objects
        """
        if not domains:
            return []

        logger.info("Starting domain validation and cleaning", total_count=len(domains))

        cleaned_domains = []

        for domain_model in domains:
            # Attempt to clean domain
            cleaned = clean_domain(domain_model.domain)

            if cleaned:
                # Update domain model with cleaned domain
                domain_model.domain = cleaned
                cleaned_domains.append(domain_model)
            else:
                logger.warning(
                    "Could not clean domain",
                    domain=domain_model.domain,
                    source=domain_model.source,
                )

        # Now validate the cleaned domains
        return self.validate(cleaned_domains)

    def get_statistics(self) -> dict:
        """
        Get validation statistics.

        Returns:
            Dictionary with validation stats
        """
        return self.stats.copy()

    def reset_statistics(self):
        """Reset validation statistics."""
        self.stats = {
            "total": 0,
            "valid": 0,
            "invalid": 0,
            "duplicates": 0,
        }


def validate_domains(
    domains: List[DomainModel], strict: bool = False
) -> Tuple[List[DomainModel], dict]:
    """
    Convenience function to validate domains.

    Args:
        domains: List of DomainModel objects
        strict: If True, raise errors on validation failure

    Returns:
        Tuple of (valid_domains, statistics)
    """
    validator = DomainValidator(strict=strict)
    valid = validator.validate(domains)
    stats = validator.get_statistics()
    return valid, stats
