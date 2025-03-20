<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class Address extends Model
{
    protected $fillable = [
        'person_id',
        'zipcode',
        'address',
        'number',
        'district',
        'city',
        'state',
        'complement',
    ];

    public function person(): BelongsTo
    {
        return $this->belongsTo(Person::class);
    }
}
