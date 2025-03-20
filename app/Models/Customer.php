<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class Customer extends Model
{
    protected $fillable = [
    ];

    public function people(): HasMany
    {
        return $this->hasMany(Person::class);
    }

    public function properties(): HasMany
    {
        return $this->hasMany(Property::class);
    }
}
