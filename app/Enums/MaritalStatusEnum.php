<?php

namespace App\Enums;

enum MaritalStatusEnum: string
{
    case Single   = 'solteiro';
    case Married  = 'casado';
    case Divorced = 'divorciado';
    case Widowed  = 'viuvo';
}
